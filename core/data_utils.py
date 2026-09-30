"""
纯数据转换工具
"""

import math

QUOTE_KEYS = [
    "datetime",
    "last_price",
    "ask_price1",
    "ask_volume1",
    "bid_price1",
    "bid_volume1",
    "volume",
    "open_interest",
]


def quote_to_dict(q):
    """TqSdk Quote 对象 → 纯数据字典（NaN / None → 0）"""
    result = {}
    for k in QUOTE_KEYS:
        v = getattr(q, k, None)
        if v is None:
            v = 0
        elif isinstance(v, float) and math.isnan(v):
            v = 0
        result[k] = v
    return result
