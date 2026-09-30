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

# ---- WS 订阅服务（旧版，已停用，保留回滚用）----
WS_CONFIG = {"host": "0.0.0.0", "port": 8765}
WS_USER_KEY = "tq:ws:users"        # 用户认证表（Redis Hash: 用户 → token）
WS_SUBS_KEY_PREFIX = "tq:ws:subs:"  # 用户订阅持久化（Redis Set）
WS_AUTH_TIMEOUT = 10               # 连接后完成认证的时限（秒）
WS_PUSH_INTERVAL = 5               # 无行情变动时重推最后快照的间隔（秒）

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
