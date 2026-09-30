"""
MQTT 价差服务：订阅 Redis 行情，计算用户订阅的跨期价差，经 Mosquitto 分发。

主题（QoS 1）：
  tq/spread/{user}/{第一腿}&{第二腿}   价差推送（retain，新订阅即得最后值）
  tq/sub/{user}                        增加订阅：{"pairs": "a&b" 或 ["a&b", ...]}（增量，不影响已有订阅）
  tq/unsub/{user}                      删除订阅：{"pairs": "a&b" 或 ["a&b", ...]}（retain 价差同步清除）
  tq/status/{user}                     处理结果（retain）：{"subscribed": [...], "invalid": [...], "pending": [...]}

注意：控制消息不要使用 retain——服务会忽略 retained 控制消息，避免历史消息重放；
     订阅的持久化由服务端 Redis 注册表负责。

说明：
  - 订阅注册表持久化在 Redis Set tq:subs:{用户}，服务重启自动恢复；旧 WS 服务遗留的
    tq:ws:subs:{用户} 首次运行自动迁移
  - 行情来源：Redis Pub/Sub shfe:quotes 增量 + Redis Hash 懒加载
  - 行情变动即时推送；超过 MQTT_PUSH_INTERVAL 未推送的对重推最后快照（更新 retain）
  - 连接认证由 Mosquitto 负责；建议为每个真实用户建独立 Mosquitto 账号并用 ACL
    限制其只能订阅 tq/spread/{用户}/#

用法: python mqtt_service.py
"""

import asyncio
import json
import logging
import time

import aiomqtt
import redis
import redis.asyncio as aioredis

from config import (
    MQTT_CONFIG,
    MQTT_PUSH_INTERVAL,
    MQTT_SUB_KEY_PREFIX,
    REDIS_CONFIG,
    REDIS_PUB_NAME,
)
from core.quote_cache import QuoteCache
from core.spread_manager import calc_spread, parse_pair

CLIENT_ID = "tq-md-spread"
CONTROL_PREFIX = "tq/sub/"      # 增加订阅
UNSUB_PREFIX = "tq/unsub/"      # 删除订阅
LEGACY_SUB_KEY_PREFIX = "tq:ws:subs:"  # 旧 WS 服务遗留注册表，首次运行自动迁移

CLIENTS = {}    # {user: set((a_code, b_code))}
PUSHED_AT = {}  # {(user, "a&b"): time.monotonic()}
BROKER = {"client": None}  # 当前 aiomqtt Client，断线期间为 None


def subs_key(user: str) -> str:
    """用户订阅注册表 Key：tq:subs:{用户}"""
    return f"{MQTT_SUB_KEY_PREFIX}{user}"


def spread_topic(user: str, pair_str: str) -> str:
    return f"tq/spread/{user}/{pair_str}"


async def current_spread(r, cache: QuoteCache, a_code: str, b_code: str):
    """取合约对当前价差；行情缺失或价格无效时返回 None。单个合约对的异常不影响其它合约对。"""
    try:
        qa, qb = await cache.get(r, a_code), await cache.get(r, b_code)
        if qa is None or qb is None:
            return None
        return calc_spread(qa, qb)
    except (KeyError, TypeError, ValueError):
        return None


async def publish_one(client, user: str, a_code: str, b_code: str, spread: dict, now: float):
    """发布单个合约对价差（retain）。断线时静默丢弃，由 push_due 重连后补发。"""
    pair_str = f"{a_code}&{b_code}"
    try:
        await client.publish(spread_topic(user, pair_str), json.dumps(spread), qos=1, retain=True)
    except aiomqtt.MqttError:
        return
    PUSHED_AT[(user, pair_str)] = now


async def publish_status(client, user: str, data: dict):
    try:
        await client.publish(f"tq/status/{user}", json.dumps(data, ensure_ascii=False), qos=1, retain=True)
    except aiomqtt.MqttError:
        pass


async def dispatch(r, cache: QuoteCache, changed: set):
    """行情变动 → 重算受影响的订阅对并发布"""
    client = BROKER["client"]
    if client is None or not changed:
        return
    now = time.monotonic()
    for user, pairs in list(CLIENTS.items()):
        for a_code, b_code in list(pairs):
            if a_code in changed or b_code in changed:
                spread = await current_spread(r, cache, a_code, b_code)
                if spread is not None:
                    await publish_one(client, user, a_code, b_code, spread, now)


async def push_due(r, cache: QuoteCache, interval: float):
    """重推超过 interval 未推送的订阅对（无新行情时即刷新最后快照）"""
    client = BROKER["client"]
    if client is None:
        return
    now = time.monotonic()
    for user, pairs in list(CLIENTS.items()):
        for a_code, b_code in sorted(pairs):
            pair_str = f"{a_code}&{b_code}"
            if now - PUSHED_AT.get((user, pair_str), 0.0) < interval:
                continue
            spread = await current_spread(r, cache, a_code, b_code)
            if spread is not None:
                await publish_one(client, user, a_code, b_code, spread, now)


async def snapshot_pusher(r, cache: QuoteCache, interval: float):
    """周期任务：保证每个行情就绪的订阅对至少每 interval 秒发布一次"""
    while True:
        await asyncio.sleep(interval)
        try:
            await push_due(r, cache, interval)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"[ MQTT ] 快照重推异常: {e}")


async def load_user_subs(r):
    """从 Redis 恢复所有用户订阅；旧 tq:ws:subs:* 首次运行自动迁移到 tq:subs:*"""
    async for key in r.scan_iter(match=f"{LEGACY_SUB_KEY_PREFIX}*"):
        user = key[len(LEGACY_SUB_KEY_PREFIX):]
        if not await r.exists(subs_key(user)):
            members = await r.smembers(key)
            if members:
                await r.sadd(subs_key(user), *members)
                logging.info(f"[ MQTT ] 已迁移用户 {user} 的 {len(members)} 条订阅（{key} → {subs_key(user)}）")
    async for key in r.scan_iter(match=f"{MQTT_SUB_KEY_PREFIX}*"):
        user = key[len(MQTT_SUB_KEY_PREFIX):]
        pairs = {p for entry in await r.smembers(key) if (p := parse_pair(entry))}
        if pairs:
            CLIENTS[user] = pairs
            logging.info(f"[ MQTT ] 恢复用户 {user} 的 {len(pairs)} 个订阅对")


def route_control(topic: str):
    """
    控制主题路由：tq/sub/{user} → (user, True) 增加订阅；
    tq/unsub/{user} → (user, False) 删除订阅；非控制主题返回 None。
    """
    for prefix, add in ((CONTROL_PREFIX, True), (UNSUB_PREFIX, False)):
        if topic.startswith(prefix):
            user = topic[len(prefix):]
            if user and not any(c in user for c in "/+#"):
                return user, add
            return None
    return None


def normalize_entries(payload):
    """
    控制消息 payload → 合约对条目列表。
    支持单个字符串或数组；无法解析返回 (None, 错误说明)。
    """
    try:
        msg = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        return None, "无效 JSON"
    if isinstance(msg, dict):
        msg = msg.get("pairs")
    if isinstance(msg, str):
        return [msg], None
    if isinstance(msg, list):
        return msg, None
    return None, '格式应为 {"pairs": "a&b" 或 ["a&b", ...]}'


async def clear_retained(client, user: str, pair_str: str):
    """删除订阅时清除该对在 broker 上的 retain 价差，避免残留旧值"""
    try:
        await client.publish(spread_topic(user, pair_str), None, qos=1, retain=True)
    except aiomqtt.MqttError:
        pass


async def apply_control(r, cache: QuoteCache, user: str, payload, add: bool):
    """
    处理订阅控制：tq/sub/{user} 增加订阅（增量，不影响已有），tq/unsub/{user} 删除订阅。
    处理结果（当前完整订阅集 + invalid + pending）经 tq/status/{user} 回发。
    """
    if payload is None or payload == b"" or payload == "":
        return  # 空消息是 retain 清除操作，静默忽略
    entries, err = normalize_entries(payload)
    if entries is None:
        await publish_status(BROKER["client"], user, {"error": err})
        return

    desired, invalid = set(), []
    for entry in entries:
        parsed = parse_pair(entry)
        if parsed is None:
            invalid.append(entry)
        else:
            desired.add(parsed)

    current = CLIENTS.get(user, set())
    client = BROKER["client"]
    now = time.monotonic()

    if add:
        for pair in desired - current:
            await r.sadd(subs_key(user), f"{pair[0]}&{pair[1]}")
        new = current | desired
    else:
        for pair in desired & current:
            await r.srem(subs_key(user), f"{pair[0]}&{pair[1]}")
            PUSHED_AT.pop((user, f"{pair[0]}&{pair[1]}"), None)
            if client is not None:
                await clear_retained(client, user, f"{pair[0]}&{pair[1]}")
        new = current - desired

    if new:
        CLIENTS[user] = new
    else:
        CLIENTS.pop(user, None)

    newly = (desired - current) if add else set()
    subscribed, pending = [], []
    for a_code, b_code in sorted(new):
        pair_str = f"{a_code}&{b_code}"
        subscribed.append(pair_str)
        spread = await current_spread(r, cache, a_code, b_code)
        if spread is None:
            pending.append(pair_str)
        elif client is not None and (a_code, b_code) in newly:
            await publish_one(client, user, a_code, b_code, spread, now)

    action = "增加" if add else "删除"
    changed = len(newly) if add else len(desired & current)
    logging.info(
        f"[ MQTT ] 用户 {user} {action} {changed} 对：现有 {len(subscribed)} 对"
        f"（pending {len(pending)}，invalid {len(invalid)}）"
    )
    await publish_status(client, user, {"subscribed": subscribed, "invalid": invalid, "pending": pending})


async def redis_quote_listener(r, cache: QuoteCache):
    """订阅 Redis 行情频道，更新缓存并触发价差发布；断线自动重连"""
    while True:
        pubsub = r.pubsub()
        try:
            await pubsub.subscribe(REDIS_PUB_NAME)
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                changed = cache.apply_pub(json.loads(msg["data"]))
                if changed:
                    await dispatch(r, cache, changed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"[ MQTT ] Redis 行情订阅中断: {e}，3 秒后重连")
        finally:
            cache.clear()  # 断线期间缓存可能陈旧，重连后靠懒加载重建
            try:
                await pubsub.aclose()
            except Exception:
                pass
        await asyncio.sleep(3)


async def broker_loop(r, cache: QuoteCache):
    """连接 Mosquitto：处理订阅控制主题；断线自动重连"""
    while True:
        try:
            async with aiomqtt.Client(
                hostname=MQTT_CONFIG["host"],
                port=MQTT_CONFIG["port"],
                username=MQTT_CONFIG.get("username"),
                password=MQTT_CONFIG.get("password"),
                identifier=CLIENT_ID,
                clean_session=False,  # 持久会话：服务离线期间的控制消息由 broker 排队补投
            ) as client:
                BROKER["client"] = client
                await client.subscribe(f"{CONTROL_PREFIX}+", qos=1)
                await client.subscribe(f"{UNSUB_PREFIX}+", qos=1)
                await load_user_subs(r)
                logging.info(
                    f"[ MQTT ] 已连接 broker {MQTT_CONFIG['host']}:{MQTT_CONFIG['port']}，"
                    f"恢复用户 {len(CLIENTS)} 个"
                )
                async for message in client.messages:
                    topic = str(message.topic)
                    if message.retain:
                        # 控制消息不使用 retain：忽略历史残留，防止旧消息重放
                        logging.info(f"[ MQTT ] 忽略 retained 控制消息 {topic}")
                        continue
                    routed = route_control(topic)
                    if routed is None:
                        continue
                    user, add = routed
                    await apply_control(r, cache, user, message.payload, add)
        except asyncio.CancelledError:
            raise
        except aiomqtt.MqttError as e:
            logging.error(f"[ MQTT ] broker 连接中断: {e}，3 秒后重连")
        except Exception as e:
            logging.exception(f"[ MQTT ] 处理异常: {e}，3 秒后重连")
        finally:
            BROKER["client"] = None
        await asyncio.sleep(3)


async def main():
    r = aioredis.Redis(**REDIS_CONFIG, decode_responses=True)
    if not await r.ping():
        raise RuntimeError("Redis 连接失败")
    logging.info("Redis 已连接")
    cache = QuoteCache()

    tasks = [
        asyncio.create_task(redis_quote_listener(r, cache)),
        asyncio.create_task(snapshot_pusher(r, cache, MQTT_PUSH_INTERVAL)),
    ]
    try:
        await broker_loop(r, cache)  # 前台运行，内建重连
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("用户中断，已退出")
