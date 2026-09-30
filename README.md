# tq-md-server

SHFE 全品种期货行情实时服务，双服务架构：

- **行情服务**（`run.py`）：基于 [TqSdk](https://www.shinnytech.com/tqsdk/) 订阅全部合约，关键行情写入 Redis Hash，并增量 Pub/Sub 推送
- **MQTT 价差服务**（`mqtt_service.py`）：行情数据从 Redis 获取，计算每个用户订阅的跨期合约价差，经 Mosquitto 分发；订阅注册表持久化在 Redis，服务重启自动恢复
- **统一入口**（`main.py`）：一个进程同时启动两个服务（行情服务为主线程，MQTT 价差服务为子线程）

## 项目结构

```
tq-md-server/
├── main.py                 # 统一入口：一个进程同时启动两个服务
├── run.py                  # 行情服务入口（可单独运行）：TqSdk → Redis
├── mqtt_service.py         # MQTT 价差服务入口（可单独运行）：Redis → Mosquitto
├── config.example.py       # 配置示例
├── config.py               # 真实配置：TqSdk 账户、Redis、Mosquitto、品种列表
├── core/
│   ├── quote_manager.py    # 合约订阅、wait_update 循环、行情 Redis 读写
│   ├── quote_cache.py      # 最新行情缓存（Pub/Sub 增量 + Hash 懒加载）
│   ├── spread_manager.py   # 跨期价差计算 & Key 工具
│   └── data_utils.py       # Quote → dict 转换工具
├── requirements.txt
└── run_service.sh          # Linux 后台管理
```

## 快速开始

### 1. 安装

```bash
pip install -r requirements.txt
```

### 2. 配置

复制 `config.example.py` 为 `config.py` 并填入真实值

```python
TQ_CONFIG = {"user_name": "快期账户", "password": "账户密码"}

REDIS_CONFIG = {
    "host": "0.0.0.0",
    "port": 6379,
    "password": "",
    "db": 0,
}
REDIS_PUB_NAME = "shfe:quotes"  # 行情 Pub/Sub 频道名

MQTT_CONFIG = {                 # Mosquitto broker
    "host": "0.0.0.0",
    "port": 1883,
    "username": "",
    "password": "",
}
MQTT_SUB_KEY_PREFIX = "tq:subs:"  # 用户订阅注册表（Redis Set: tq:subs:{用户}）
MQTT_PUSH_INTERVAL = 5            # 无行情变动时重推最后快照的间隔（秒）
```

### 3. 运行

```bash
python main.py                # 统一启动（推荐）：行情服务（主线程）+ MQTT 价差服务（子线程）
python run.py                 # 仅行情服务（前台）
python mqtt_service.py        # 仅 MQTT 价差服务（需 Redis 中已有行情，或先启动行情服务）

./run_service.sh start        # Linux 后台（运行统一入口 main.py）
./run_service.sh stop         # 停止
./run_service.sh status       # 状态
```

## 数据流程

```
TqSdk 行情源
  │ query_quotes (20品种所有合约)
  │ wait_update() → is_changing() 检测变动
  ▼
quote_to_dict() 提取关键行情字段（NaN → 0）
  ▼
Redis Hash（SHFE:{品种}:{合约}）+ Pub/Sub（shfe:quotes）
  ▼
mqtt_service.py
  │ 行情缓存（Pub/Sub 增量 + Hash 懒加载）→ calc_spread()（仅用户订阅的合约对）
  ▼
Mosquitto：tq/spread/{user}/{合约对}（retain，QoS 1）→ 订阅该主题的客户端
```

## 行情字段

| 字段                          | 说明            |
| ----------------------------- | --------------- |
| `datetime`                    | 行情时间        |
| `last_price`                  | 最新价          |
| `bid_price1` / `ask_price1`   | 买一价 / 卖一价 |
| `bid_volume1` / `ask_volume1` | 买一量 / 卖一量 |
| `volume`                      | 成交量          |
| `open_interest`               | 持仓量          |

价格为无效值（NaN，如未开盘、涨跌停无对手价）时统一写 0。

## Redis 数据格式

**行情 Key：** `SHFE:{品种}:{合约}`

```
HGETALL SHFE:ag:ag2610
datetime        2026-09-29 21:00:00.500000
last_price      8215.0
bid_price1      8214.0
ask_price1      8216.0
bid_volume1     12
ask_volume1     30
volume          123456
open_interest   456789.0
```

**Pub/Sub 消息**（频道 `shfe:quotes`）：`[{"ag2610": {上述字段...}}, ...]`

## MQTT 接口

Broker：`mqtt.example.com:1883`（替换为你自己的 broker 地址）。MQTT 为二进制协议，不走 HTTP、没有 Host 头，**不受域名备案拦截**，可直接用域名连接。

### 主题

| 主题                                 | 方向          | 说明                                                                   |
| ------------------------------------ | ------------- | ---------------------------------------------------------------------- |
| `tq/spread/{user}/{第一腿}&{第二腿}` | 服务 → 客户端 | 四向价差（retain + QoS 1），新订阅即得最后值                           |
| `tq/sub/{user}`                      | 客户端 → 服务 | **增加**订阅：`{"pairs": "a&b" 或 ["a&b", ...]}`，增量生效、不影响已有 |
| `tq/unsub/{user}`                    | 客户端 → 服务 | **删除**订阅：payload 同上，移除指定对并清除其 retain 价差             |
| `tq/status/{user}`                   | 服务 → 客户端 | 处理结果（retain）：`subscribed` / `invalid` / `pending`               |

### 使用流程

1. 增加订阅（单个或多个，增量生效；**控制消息不要 retain**）：

   topic：`tq/sub/u1`，payload：`{"pairs": "ag2612&ag2610"}` 或 `{"pairs": ["ag2612&ag2610", "cu2608&cu2606"]}`

2. 删除订阅（该对在 broker 上的 retain 价差同步清除，新订阅者不会再收到）：

   topic：`tq/unsub/u1`，payload 同上

3. 服务处理完成在 `tq/status/u1` 回发**当前完整订阅**：`{"subscribed": [...], "invalid": [...], "pending": [...]}`。`invalid` 为格式错误，`pending` 为行情尚未就绪（已持久化，行情到达后自动开始推送）
4. 订阅 `tq/spread/u1/#` 接收价差推送：任一腿变动即时发布；行情安静时每 `MQTT_PUSH_INTERVAL`（默认 5）秒重推最后快照（更新 retain）

> 服务会忽略 retained 与空 payload 的控制消息（防历史消息重放）；若之前发过 retained 控制消息，向原主题发空 payload 即可清除。

价差 payload（主题即合约对）：

```json
{ "short_open": -15.0, "long_open": 12.0, "short_cover": 12.0, "long_cover": -15.0 }
```

### 客户端建议

- 固定 client id + `clean_session=false`（MQTT 持久会话）：broker 记住订阅，客户端离线期间的 QoS 1 消息自动补推
- 订阅 `tq/spread/{user}/#` 与 `tq/status/{user}`
- 测试工具用 [MQTTX](https://mqttx.app/)

### 安全建议（按用户隔离）

连接认证由 Mosquitto 负责。为每个真实用户建独立账号并用 ACL 隔离（服务端操作）：

```bash
sudo mosquitto_passwd /etc/mosquitto/passwd u1          # 建账号（ACL 用 %u pattern，加用户无需改 ACL）
sudo tee /etc/mosquitto/aclfile >/dev/null <<'EOF'
pattern write tq/sub/%u
pattern write tq/unsub/%u
pattern read  tq/spread/%u/#
pattern read  tq/status/%u

user <服务账号>
topic readwrite tq/#
EOF
# 在 listener 配置块加: acl_file /etc/mosquitto/aclfile
sudo systemctl reload mosquitto
```

规则：普通用户只能发布 `tq/sub/{自己}`、只能订阅 `tq/spread/{自己}/#` 与 `tq/status/{自己}`；服务账号仅供服务端使用，客户端一律用自己的账号，主题中的用户名必须与 Mosquitto 用户名一致。公网长期暴露建议启用 TLS。

### 价差公式

a = 第一腿（远月），b = 第二腿（近月）。价格为买一/卖一。

| 字段          | 方向 | 操作      | 公式            |
| ------------- | ---- | --------- | --------------- |
| `short_open`  | 空开 | 卖 a 买 b | `a.bid − b.ask` |
| `long_open`   | 多开 | 买 a 卖 b | `a.ask − b.bid` |
| `short_cover` | 空平 | 买 a 卖 b | `a.ask − b.bid` |
| `long_cover`  | 多平 | 卖 a 买 b | `a.bid − b.ask` |

`short_open` ≡ `long_cover`，`long_open` ≡ `short_cover`（公式相同）。

## 订阅品种

上期所全部 20 个期货品种：

```
ad  ag  al  ao  au  br  bu  cu  fu  hc
ni  op  pb  rb  ru  sn  sp  ss  wr  zn
```

## 技术栈

- **TqSdk** — 行情数据源
- **Redis** — Hash 存储 + 行情 Pub/Sub + 订阅注册表
- **Mosquitto + aiomqtt** — MQTT broker 与客户端
