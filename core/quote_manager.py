"""
行情订阅 & Redis 写入管理
"""

import json
import logging
import time
from datetime import datetime, timedelta

import redis
from tqsdk import TqApi
from config import EXCHANGE, PRODUCTS, QUERY_CONFIG, QUOTE_CLEAN_INTERVAL, QUOTE_STALE_DAYS, REDIS_PUB_NAME

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


def clean_expired_quotes(r: redis.Redis, all_quotes: dict, stale_days: int = QUOTE_STALE_DAYS, api=None):
    """
    清理 Redis 行情存储中的过期数据：
      - 当前订阅之外的合约 Key（历史运行残留、已摘牌合约）→ 删除
      - 订阅内但行情超过 stale_days 个交易日未更新的（本运行期间过期）→ 删除；
        提供 api 时按交易日历计时，休市日（节假日/周末）不计入，避免长假误清
      - 旧版价差 Key（合约段含 &）→ 删除
    datetime 无法解析的保留，等正常行情覆盖。返回删除条数。
    """
    active_codes = {c.split(".")[1] for c in all_quotes}
    cutoff = datetime.now() - timedelta(days=stale_days)
    pipe = r.pipeline(transaction=False)
    removed = 0
    stale_candidates = {}  # {key: datetime} 自然日超期候选，待交易日历精确判定

    for key in r.scan_iter(match=f"{EXCHANGE}:*:*"):
        parts = key.split(":")
        if len(parts) != 3:
            continue
        code = parts[2]
        if "&" in code or code not in active_codes:
            pipe.delete(key)
            removed += 1
            continue
        dt_str = r.hget(key, "datetime")
        if dt_str:
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                if dt < cutoff:
                    stale_candidates[key] = dt  # 交易日数 >= stale_days 才删，粗筛自然日下界不会漏删
            except ValueError:
                pass

    if stale_candidates:
        today = datetime.now().date()
        expired_keys = _judge_stale_by_trading_days(stale_candidates, stale_days, today, api)
        for key in expired_keys:
            pipe.delete(key)
            removed += 1

    if removed:
        pipe.execute()
    logging.info(f"[ 清理 ] 过期行情清理：删除 {removed} 个 Hash（休市日不计入过期计时）")
    return removed


def _judge_stale_by_trading_days(stale_candidates: dict, stale_days: int, today, api) -> list:
    """
    交易日判定：(行情日期, today] 区间内交易日数 >= stale_days 才算过期。
    api 不可用或日历查询失败时回退自然日判定（全部视为过期候选）。
    """
    if api is None:
        return list(stale_candidates)
    try:
        earliest = min(stale_candidates.values()).date()
        calendar = api.get_trading_calendar(start_dt=earliest, end_dt=today)
        trading_days = {d.date() for d in calendar[calendar["trading"]]["date"]}
        return [
            key for key, dt in stale_candidates.items()
            if sum(1 for d in trading_days if dt.date() < d <= today) >= stale_days
        ]
    except Exception as e:
        logging.warning(f"[ 清理 ] 交易日历查询失败，本轮按自然日判定: {e}")
        return list(stale_candidates)


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
    clean_expired_quotes(r, all_quotes, QUOTE_STALE_DAYS, api=api)
    last_clean = time.monotonic()

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
        if time.monotonic() - last_clean >= QUOTE_CLEAN_INTERVAL:
            clean_expired_quotes(r, all_quotes, QUOTE_STALE_DAYS, api=api)
            last_clean = time.monotonic()
