"""
跨期价差计算 & 行情 Key 工具（行情服务与 MQTT 价差服务共享）
"""

import math
import re
from itertools import combinations

from config import EXCHANGE

PID_RE = re.compile(r"[A-Za-z]+")
CODE_RE = re.compile(r"[A-Za-z]+\d+")
PRODUCT_RE = re.compile(r"[A-Za-z]+")


def parse_entry(entry):
    """
    订阅条目分类：("product", "ag") 品种 / ("pair", (a_code, b_code)) 合约对 / None。
    含 & 的按合约对解析，纯字母按品种解析。
    """
    if not isinstance(entry, str):
        return None
    if "&" in entry:
        parsed = parse_pair(entry)
        return ("pair", parsed) if parsed else None
    if PRODUCT_RE.fullmatch(entry):
        return ("product", entry)
    return None


def product_pairs(codes) -> list:
    """
    品种全部在市合约的跨期组合，远月在前（a=远月，b=近月，与价差公式约定一致）。
    codes: 合约代码集合，如 {"ag2610", "ag2612", "ag2702"}，
    返回 [("ag2702", "ag2612"), ("ag2702", "ag2610"), ("ag2612", "ag2610"), ...]
    """
    ordered = sorted(codes, key=lambda c: int(re.search(r"\d+", c).group()), reverse=True)
    return list(combinations(ordered, 2))


def quote_key(code: str):
    """行情 Hash Key：SHFE:{品种}:{合约}。合约代码非法时返回 None。"""
    m = PID_RE.match(code)
    if not m or not CODE_RE.fullmatch(code):
        return None
    return f"{EXCHANGE}:{m.group()}:{code}"


def parse_pair(entry: str):
    """
    解析跨期合约对 "第一腿&第二腿"。
    返回 (a_code, b_code)，格式错误返回 None。
    """
    if not isinstance(entry, str):
        return None
    try:
        a_code, b_code = entry.split("&")
    except ValueError:
        return None
    if a_code == b_code or not CODE_RE.fullmatch(a_code) or not CODE_RE.fullmatch(b_code):
        return None
    return a_code, b_code


def calc_spread(a: dict, b: dict):
    """
    计算跨期价差四向值。
    a = 第一腿（远月），b = 第二腿（近月），入参为行情字典。
    价格无效（0/NaN，如未开盘、涨跌停无对手价）时返回 None。
    """
    a_bid = a["bid_price1"]
    a_ask = a["ask_price1"]
    b_bid = b["bid_price1"]
    b_ask = b["ask_price1"]

    if any(v == 0 or (isinstance(v, float) and math.isnan(v)) for v in (a_bid, a_ask, b_bid, b_ask)):
        return None

    short_open = round(a_bid - b_ask, 2)
    long_open = round(a_ask - b_bid, 2)

    return {
        "short_open": short_open,   # 空开：卖a买b
        "long_open": long_open,     # 多开：买a卖b
        "short_cover": long_open,   # 空平：买a卖b ≡ long_open
        "long_cover": short_open,   # 多平：卖a买b ≡ short_open
    }
