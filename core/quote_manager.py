"""
行情订阅 & Redis 写入管理
"""

import json
import logging
import time
import redis
from tqsdk import TqApi
from config import EXCHANGE, PRODUCTS, QUERY_CONFIG, REDIS_PUB_NAME

from core.data_utils import quote_to_dict


# ---- 订阅 ------------------------------------------------------------------
def subscribe(api: TqApi, product_id: str):
    """查询未过期合约并全部订阅，返回 [quote_obj]"""
    contracts = api.query_quotes(
        **QUERY_CONFIG,
        product_id=product_id,
    )
    return api.get_quote_list(contracts)


# ---- Redis 写入 --------------------------------------------------------------
def store_quotes(r: redis.Redis, all_quotes: dict, contract_product: dict):
    """全量写入所有合约关键行情到 Redis。返回写入条数。"""
    pipe = r.pipeline(transaction=False)
    total = 0

    for contract, q in all_quotes.items():
        pid = contract_product[contract]
        code = contract.split(".")[1]
        pipe.hset(f"{EXCHANGE}:{pid}:{code}", mapping=quote_to_dict(q))
        total += 1

    pipe.execute()
    logging.info(f"行情全量写入 Redis，共 {total} 条")
    return total


def update_quotes_incremental(
    r: redis.Redis,
    changed_symbols: set,
    all_quotes: dict,
    contract_product: dict,
    pub_channel: str | None = None,
):
    """
    增量更新变动合约的关键行情。
    pub_channel 非空时将本轮所有变动合约发布到 Pub/Sub。
    返回更新条数。
    """
    pipe = r.pipeline(transaction=False)
    pub_msgs = []

    for contract in changed_symbols:
        q = all_quotes.get(contract)
        if q is None:
            continue

        pid = contract_product[contract]
        code = contract.split(".")[1]
        data = quote_to_dict(q)
        pipe.hset(f"{EXCHANGE}:{pid}:{code}", mapping=data)
        pub_msgs.append({code: data})

    if pub_msgs:
        if pub_channel:
            pipe.publish(pub_channel, json.dumps(pub_msgs))
        pipe.execute()
    return len(pub_msgs)


# ---- 增量监听循环 -----------------------------------------------------------
def run_update_loop(api: TqApi, r: redis.Redis):
    """阻塞运行行情增量写入循环"""
    all_quotes = {}  # {full_symbol: quote_obj}
    contract_product = {}  # {full_symbol: product_id}

    t0 = time.perf_counter()
    for pid in PRODUCTS:
        start = time.perf_counter()
        quotes = subscribe(api, pid)
        end = time.perf_counter()
        logging.info(f"[ {pid} ] 品种合约订阅完成。耗时 {end - start:.4f}s")

        for q in quotes:
            contract = q["instrument_id"]
            all_quotes[contract] = q
            contract_product[contract] = pid

    api.wait_update()  # 等待首批行情
    store_quotes(r, all_quotes, contract_product)

    t1 = time.perf_counter()
    logging.info(f"订阅初始化完毕。共耗时 {t1 - t0:.4f}s")
    logging.info("=" * 60)
    logging.info(f"开始监听 {len(PRODUCTS)} 个品种, {len(all_quotes)} 个合约")
    while api.wait_update():
        start = time.perf_counter()
        changed_symbols = {c for c, q in all_quotes.items() if api.is_changing(q)}
        if changed_symbols:
            update_quotes_incremental(
                r, changed_symbols, all_quotes, contract_product,
                pub_channel=REDIS_PUB_NAME,
            )
            end = time.perf_counter()
            logging.info(f"{len(changed_symbols)} 个合约变动，行情更新 {end - start:.4f}s")
