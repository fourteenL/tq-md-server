"""
最新行情缓存：Pub/Sub 增量更新 + Redis Hash 懒加载
"""

from core.spread_manager import quote_key

# Redis Hash 行情字段类型（decode_responses=True 读回全是 str）
FLOAT_KEYS = {"last_price", "ask_price1", "bid_price1", "open_interest"}
INT_KEYS = {"ask_volume1", "bid_volume1", "volume"}


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
