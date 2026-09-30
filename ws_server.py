"""
WS 订阅服务：用户认证后通过 WebSocket 订阅跨期合约对，服务端从 Redis 获取行情、
计算四向价差并按连接单独推送。

  - 认证：URL 参数 ?user=&token=（推荐，连接即认证）或首条消息 auth；
    超时或失败即断开；用户表存于 Redis Hash tq:ws:users（用户 → token）
  - 持久化：订阅/取消订阅实时写入 Redis Set tq:ws:subs:{用户}，
    重连认证成功后自动恢复订阅并推送当前快照

协议（JSON）：
  认证（二选一）:
    连接 URL 携带参数（推荐）: ws://host:port/?user=u1&token=xxx
    或连接后首条消息（10 秒内）: {"action": "auth", "user": "u1", "token": "xxx"}
  客户端 → 服务端:
    {"action": "subscribe",   "pairs": ["ag2612&ag2610", ...]}
    {"action": "unsubscribe", "pairs": ["ag2612&ag2610", ...]}
  服务端 → 客户端:
    {"event": "authenticated", "user": "u1", "subscribed": [...], "invalid": [...], "pending": [...], "snapshot": {...}}
    {"event": "subscribed", "subscribed": [...], "invalid": [...], "pending": [...], "snapshot": {...}}
    {"event": "unsubscribed", "subscribed": [...]}
    {"event": "spread", "data": [{合约对: 价差}, ...]}
    {"event": "error", "message": "..."}

用法: python ws_server.py
"""

import asyncio
import dataclasses
import hmac
import json
import logging
import time
from urllib.parse import parse_qs, urlparse

import redis
import redis.asyncio as aioredis
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from config import (
    REDIS_CONFIG,
    REDIS_PUB_NAME,
    WS_AUTH_TIMEOUT,
    WS_CONFIG,
    WS_PUSH_INTERVAL,
    WS_SUBS_KEY_PREFIX,
    WS_USER_KEY,
)
from core.spread_manager import calc_spread, parse_pair, quote_key


@dataclasses.dataclass
class ClientConn:
    """单个连接的运行状态：订阅集合 + 各合约对最近一次推送时间"""

    subs: set = dataclasses.field(default_factory=set)       # {(a_code, b_code)}
    pushed_at: dict = dataclasses.field(default_factory=dict)  # {"a&b": time.monotonic()}


CLIENTS = {}  # {ws连接: ClientConn}

# Redis Hash 行情字段类型（decode_responses=True 读回全是 str）
FLOAT_KEYS = {"last_price", "ask_price1", "bid_price1", "open_interest"}
INT_KEYS = {"ask_volume1", "bid_volume1", "volume"}


def subs_key(user: str) -> str:
    """用户订阅持久化 Key：tq:ws:subs:{用户}"""
    return f"{WS_SUBS_KEY_PREFIX}{user}"


def quote_from_hash(fields: dict) -> dict:
    """Redis Hash 字段（str）→ 类型化行情字典"""
    result = {}
    for k, v in fields.items():
        if k in FLOAT_KEYS:
            result[k] = float(v) if v else 0.0
        elif k in INT_KEYS:
            result[k] = int(float(v)) if v else 0
        else:
            result[k] = v
    return result


class QuoteCache:
    """最新行情缓存：Pub/Sub 增量更新 + Redis Hash 懒加载"""

    def __init__(self):
        self._quotes = {}      # code → 行情字典
        self._missing = set()  # 已确认 Hash 中不存在的合约，避免反复查询；断线重连时清空

    def apply_pub(self, data: list) -> set:
        """应用 shfe:quotes Pub/Sub 推送，返回本轮变动合约代码集合"""
        changed = set()
        for item in data:
            for code, fields in item.items():
                self._quotes[code] = fields
                changed.add(code)
        return changed

    async def get(self, r, code: str):
        """取合约行情；缓存未命中时从 Redis Hash 懒加载"""
        if code not in self._quotes:
            if code in self._missing:
                return None
            key = quote_key(code)
            if key:
                fields = await r.hgetall(key)
                if fields:
                    self._quotes[code] = quote_from_hash(fields)
                    return self._quotes[code]
            self._missing.add(code)
        return self._quotes.get(code)

    def clear(self):
        self._quotes.clear()
        self._missing.clear()


async def current_spread(r, cache: QuoteCache, a_code: str, b_code: str):
    """取合约对当前价差；行情缺失或价格无效时返回 None。单个合约对的异常不影响其它合约对。"""
    try:
        qa, qb = await cache.get(r, a_code), await cache.get(r, b_code)
        if qa is None or qb is None:
            return None
        return calc_spread(qa, qb)
    except (KeyError, TypeError, ValueError):
        return None


def parse_url_auth(ws):
    """
    从连接 URL 查询参数解析认证信息（?user=xxx&token=yyy）。
    返回 (user, token)；URL 未携带认证参数返回 None。
    """
    request = getattr(ws, "request", None)
    path = getattr(request, "path", "") if request is not None else ""
    query = urlparse(path).query
    if not query:
        return None
    params = parse_qs(query)
    if "user" not in params and "token" not in params:
        return None
    return (params.get("user", [None])[0], params.get("token", [None])[0])


async def check_token(r, user, token) -> str | None:
    """校验用户名与 token（Redis Hash tq:ws:users）。成功返回用户名，失败返回 None。"""
    if not isinstance(user, str) or not user or not isinstance(token, str):
        return None
    stored = await r.hget(WS_USER_KEY, user)
    if stored is None:
        return None
    if not hmac.compare_digest(stored.encode(), token.encode()):
        return None
    return user


async def authenticate(r, msg) -> str | None:
    """从认证消息中提取用户名与 token 并校验。成功返回用户名，失败返回 None。"""
    user = msg.get("user") if isinstance(msg, dict) else None
    token = msg.get("token") if isinstance(msg, dict) else None
    return await check_token(r, user, token)


async def restore_user_subs(r, cache: QuoteCache, subs: set, user: str) -> dict:
    """
    从 Redis 恢复该用户持久化的订阅：全部有效对加入 subs；
    行情就绪的对附带快照，未就绪的列入 pending（行情到达后自动开始推送）；
    格式错误的对保留在 Redis 中并列入 invalid。
    返回 {"subscribed": [...], "invalid": [...], "pending": [...], "snapshot": {...}}
    """
    subscribed, invalid, pending, snapshot = [], [], [], {}
    for entry in sorted(await r.smembers(subs_key(user))):
        parsed = parse_pair(entry)
        if parsed is None:
            invalid.append(entry)
            continue
        a_code, b_code = parsed
        pair_str = f"{a_code}&{b_code}"
        subs.add((a_code, b_code))
        subscribed.append(pair_str)
        spread = await current_spread(r, cache, a_code, b_code)
        if spread is not None:
            snapshot[pair_str] = spread
        else:
            pending.append(pair_str)
    return {"subscribed": subscribed, "invalid": invalid, "pending": pending, "snapshot": snapshot}


async def process_message(r, cache: QuoteCache, subs: set, msg: dict, user: str | None = None) -> list:
    """
    处理一条客户端消息，返回待发送的服务端消息列表（不含序列化）。
    subs: 当前连接已订阅的 {(a_code, b_code)} 集合（原地修改）。
    user: 认证通过的用户名；订阅变更实时持久化到 Redis。
    """
    action = msg.get("action")
    entries = msg.get("pairs")

    if action == "auth":
        return [{"event": "error", "message": "已认证，无需重复认证"}]
    if action not in ("subscribe", "unsubscribe") or not isinstance(entries, list):
        return [{"event": "error", "message": '消息格式应为 {"action": "subscribe|unsubscribe", "pairs": ["a&b", ...]}'}]
    if user is None:
        return [{"event": "error", "message": "请先认证"}]

    if action == "unsubscribe":
        for entry in entries:
            parsed = parse_pair(entry)
            if parsed:
                subs.discard(parsed)
                await r.srem(subs_key(user), f"{parsed[0]}&{parsed[1]}")
        return [{"event": "unsubscribed", "subscribed": [f"{a}&{b}" for a, b in sorted(subs)]}]

    # ---- subscribe：格式合法的合约对一律订阅并持久化；行情未就绪的列入 pending ----
    subscribed, invalid, pending, snapshot = [], [], [], {}
    for entry in entries:
        parsed = parse_pair(entry)
        if parsed is None:
            invalid.append(entry)  # 格式错误，无法解析，不持久化
            continue
        a_code, b_code = parsed
        pair_str = f"{a_code}&{b_code}"
        if (a_code, b_code) not in subs:
            subs.add((a_code, b_code))
            subscribed.append(pair_str)
            await r.sadd(subs_key(user), pair_str)
        spread = await current_spread(r, cache, a_code, b_code)
        if spread is not None:
            snapshot[pair_str] = spread
        else:
            pending.append(pair_str)  # 行情尚未就绪，行情到达后自动开始推送
    return [{"event": "subscribed", "subscribed": subscribed, "invalid": invalid, "pending": pending, "snapshot": snapshot}]


async def _send_spread(ws, conn: ClientConn, msgs: list, now: float) -> bool:
    """向单个连接发送价差消息，成功后记录各对的推送时间。失败返回 False。"""
    try:
        await asyncio.wait_for(ws.send(json.dumps({"event": "spread", "data": msgs}, ensure_ascii=False)), 5)
    except (ConnectionClosed, asyncio.TimeoutError, OSError):
        return False
    for m in msgs:
        conn.pushed_at[next(iter(m))] = now
    return True


async def dispatch(r, cache: QuoteCache, clients: dict, changed: set):
    """向订阅了本轮变动合约的连接推送对应价差"""
    now = time.monotonic()
    for ws, conn in list(clients.items()):
        hits = [pair for pair in conn.subs if pair[0] in changed or pair[1] in changed]
        if not hits:
            continue

        msgs = []
        for a_code, b_code in hits:
            spread = await current_spread(r, cache, a_code, b_code)
            if spread is not None:
                msgs.append({f"{a_code}&{b_code}": spread})
        if not msgs:
            continue

        if not await _send_spread(ws, conn, msgs, now):
            clients.pop(ws, None)
            logging.warning(f"[ WS ] 推送失败，移除连接 {getattr(ws, 'remote_address', '?')}")


async def push_due(r, cache: QuoteCache, clients: dict, interval: float):
    """重推超过 interval 未推送过的订阅对（无新行情时即重复最后快照）"""
    now = time.monotonic()
    for ws, conn in list(clients.items()):
        due = [pair for pair in sorted(conn.subs)
               if now - conn.pushed_at.get(f"{pair[0]}&{pair[1]}", 0.0) >= interval]
        if not due:
            continue

        msgs = []
        for a_code, b_code in due:
            spread = await current_spread(r, cache, a_code, b_code)
            if spread is not None:
                msgs.append({f"{a_code}&{b_code}": spread})
        if not msgs:
            continue  # 行情未就绪（pending）的合约对不推送，也不记录时间

        if not await _send_spread(ws, conn, msgs, now):
            clients.pop(ws, None)
            logging.warning(f"[ WS ] 快照重推失败，移除连接 {getattr(ws, 'remote_address', '?')}")


async def snapshot_pusher(r, cache: QuoteCache, clients: dict, interval: float):
    """周期任务：保证每个行情就绪的订阅对至少每 interval 秒推送一次"""
    while True:
        await asyncio.sleep(interval)
        try:
            await push_due(r, cache, clients, interval)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"[ WS ] 快照重推异常: {e}")


async def auth_by_message(ws, r) -> str | None:
    """消息认证：第一条消息必须为 auth，超时或失败断开。成功返回用户名，失败返回 None。"""
    try:
        raw = await asyncio.wait_for(ws.recv(), WS_AUTH_TIMEOUT)
    except (asyncio.TimeoutError, TimeoutError):
        await ws.close(code=4001, reason="认证超时")
        return None

    msg = None
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        pass
    try:
        user = await authenticate(r, msg)
    except redis.exceptions.RedisError as e:
        logging.error(f"[ WS ] 认证读取 Redis 失败: {e}")
        await ws.close(code=1011, reason="数据源不可用")
        return None
    if user is None:
        logging.warning(f"[ WS ] 认证失败 {ws.remote_address} user={msg.get('user') if isinstance(msg, dict) else '?'}")
        await ws.send(json.dumps({"event": "error", "message": '认证失败：连接 URL 可带 ?user=&token=，或首条消息为 {"action": "auth", "user": ..., "token": ...}'}))
        await ws.close(code=4003, reason="认证失败")
        return None
    return user


async def handler(ws, r, cache: QuoteCache):
    conn = ClientConn()
    CLIENTS[ws] = conn
    logging.info(f"[ WS ] 连接建立 {ws.remote_address}")
    user = None
    try:
        # ---- 认证：URL ?user=&token= 优先；否则要求首条消息为 auth ----
        url_auth = parse_url_auth(ws)
        if url_auth is not None:
            try:
                user = await check_token(r, *url_auth)
            except redis.exceptions.RedisError as e:
                logging.error(f"[ WS ] 认证读取 Redis 失败: {e}")
                await ws.close(code=1011, reason="数据源不可用")
                return
            if user is None:
                logging.warning(f"[ WS ] 认证失败 {ws.remote_address} user={url_auth[0]} (URL)")
                await ws.close(code=4003, reason="认证失败")
                return
        else:
            user = await auth_by_message(ws, r)
            if user is None:
                return

        # ---- 恢复持久化订阅 ----
        try:
            restored = await restore_user_subs(r, cache, conn.subs, user)
        except redis.exceptions.RedisError as e:
            logging.error(f"[ WS ] 恢复订阅读取 Redis 失败: {e}")
            await ws.close(code=1011, reason="数据源不可用")
            return
        logging.info(f"[ WS ] 用户 {user} 认证成功 {ws.remote_address}，恢复订阅 {len(restored['subscribed'])} 对")
        await ws.send(json.dumps({"event": "authenticated", "user": user, **restored}, ensure_ascii=False))

        # ---- 消息循环 ----
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                await ws.send(json.dumps({"event": "error", "message": "无效 JSON"}))
                continue
            if not isinstance(msg, dict):
                await ws.send(json.dumps({"event": "error", "message": "消息应为 JSON 对象"}))
                continue

            try:
                outs = await process_message(r, cache, conn.subs, msg, user)
            except redis.exceptions.RedisError as e:
                logging.error(f"[ WS ] Redis 读取失败: {e}")
                await ws.send(json.dumps({"event": "error", "message": "数据源暂不可用，请稍后重试"}))
                continue

            for out in outs:
                await ws.send(json.dumps(out, ensure_ascii=False))
    except ConnectionClosed:
        pass
    finally:
        CLIENTS.pop(ws, None)
        logging.info(f"[ WS ] 连接断开 user={user} {ws.remote_address} code={ws.close_code}")


async def pubsub_listener(r, cache: QuoteCache):
    """订阅 shfe:quotes，更新缓存并触发推送；断线自动重连"""
    while True:
        pubsub = r.pubsub()
        try:
            await pubsub.subscribe(REDIS_PUB_NAME)
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                changed = cache.apply_pub(json.loads(msg["data"]))
                if changed:
                    await dispatch(r, cache, CLIENTS, changed)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"[ WS ] Redis 订阅中断: {e}，3 秒后重连")
        finally:
            cache.clear()  # 断线期间缓存可能陈旧，重连后靠懒加载重建
            try:
                await pubsub.aclose()
            except Exception:
                pass
        await asyncio.sleep(3)  # 正常断开时也稍作等待再重连


async def main():
    r = aioredis.Redis(**REDIS_CONFIG, decode_responses=True)
    if not await r.ping():
        raise RuntimeError("Redis 连接失败")
    logging.info("Redis 已连接")
    cache = QuoteCache()

    try:
        listener = asyncio.create_task(pubsub_listener(r, cache))
        pusher = asyncio.create_task(snapshot_pusher(r, cache, CLIENTS, WS_PUSH_INTERVAL))
        async with serve(lambda ws: handler(ws, r, cache), WS_CONFIG["host"], WS_CONFIG["port"]):
            logging.info(
                f"WS 订阅服务已启动，监听 {WS_CONFIG['host']}:{WS_CONFIG['port']}；"
                f"客户端连接 ws://127.0.0.1:{WS_CONFIG['port']}（本机）或 ws://服务器IP:{WS_CONFIG['port']}（远程），"
                f"0.0.0.0 仅是监听地址不可直接连接；无行情变动时每 {WS_PUSH_INTERVAL}s 重推最后快照"
            )
            await asyncio.gather(listener, pusher)
    except OSError as e:
        logging.error(f"[ WS ] 端口 {WS_CONFIG['port']} 监听失败（被占用或无权限？）: {e}")
        raise
    finally:
        listener.cancel()
        pusher.cancel()
        await asyncio.gather(listener, pusher, return_exceptions=True)
        await r.aclose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("用户中断，已退出")
