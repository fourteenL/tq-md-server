"""
项目配置示例：复制为 config.py 并填入真实值。
config.py 已被 .gitignore 排除，不会提交到仓库。
"""

# ---- 快期账户 ----
TQ_CONFIG = {
    "user_name": "快期账户",
    "password": "账户密码",
}

# ---- Redis ----
REDIS_CONFIG = {
    "host": "127.0.0.1",
    "port": 6379,
    "password": "",
    "db": 0,
}
REDIS_PUB_NAME = "shfe:quotes"  # 行情 Pub/Sub 频道名

# ---- 行情存储清理 ----
QUOTE_STALE_DAYS = 7          # 行情超过 N 个交易日未更新视为过期（休市日不计入，长假不误清）
QUOTE_CLEAN_INTERVAL = 21600  # 清理周期（秒），默认 6 小时

# ---- MQTT 价差服务（Mosquitto）----
MQTT_CONFIG = {
    "host": "127.0.0.1",
    "port": 1883,
    "username": "mqtt账号",
    "password": "mqtt密码",
}
MQTT_SUB_KEY_PREFIX = "tq:subs:"  # 用户订阅注册表（Redis Set: tq:subs:{用户}）
MQTT_PUSH_INTERVAL = 5            # 无行情变动时重推最后快照的间隔（秒）

# ---- 订阅设置 ----
EXCHANGE = "SHFE"  # 交易所
QUERY_CONFIG = {
    "ins_class": "FUTURE",  # 查询类型
    "exchange_id": EXCHANGE,
    "expired": False,  # 过期合约
}
# 订阅品种（上期所全部 20 个品种）
PRODUCTS = [
    "ad",
    "ag",
    "al",
    "ao",
    "au",
    "br",
    "bu",
    "cu",
    "fu",
    "hc",
    "ni",
    "op",
    "pb",
    "rb",
    "ru",
    "sn",
    "sp",
    "ss",
    "wr",
    "zn",
]
