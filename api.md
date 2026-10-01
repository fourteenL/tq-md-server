# tq-md-server 对接文档（MQTT）

> 版本：1.0 ｜ 更新日期：2026-10-01 ｜ 接入方式：MQTT 3.1.1 ｜ Broker：`tcp://<broker地址>:<端口>`（地址与账号由服务方提供）

本服务基于 TqSdk 采集上期所（SHFE）全品种期货合约行情，计算**跨期合约价差**并通过 Mosquitto（MQTT）按用户分发。客户端只需一个 MQTT 连接：发布订阅控制消息，接收价差推送。

## 1. 通用说明

### 1.1 鉴权

- 连接鉴权由 Mosquitto broker 完成：使用服务方分配的 **MQTT 用户名 / 密码** 连接（每个用户独立账号）。
- 服务端按主题中的 `{user}` 段区分数据归属：客户端只能消费 `tq/spread/<自己的用户名>/#`。
- 服务方可能启用 ACL：普通用户仅允许发布 `tq/sub/<自己的用户名>`、`tq/unsub/<自己的用户名>`，仅允许订阅 `tq/spread/<自己的用户名>/#`、`tq/status/<自己的用户名>`。

### 1.2 消息约定

| 约定 | 说明 |
| --- | --- |
| 编码 | 所有 payload 均为 UTF-8 JSON |
| QoS | 推荐 QoS 1；服务端所有发布均为 QoS 1 |
| retain | 价差与状态主题为 retain 消息（新订阅即得最后值）；**控制消息不要设置 retain**（服务端忽略 retained 控制消息，防历史重放） |
| 重复与乱序 | QoS 1 可能重复投递；同一主题以最后收到的消息为准即可 |
| 心跳 | MQTT 协议层 keepalive，客户端设 30~60 秒即可 |

### 1.3 主题总览

| 主题 | 方向 | QoS | retain | 说明 |
| --- | --- | --- | --- | --- |
| `tq/spread/{user}/{第一腿}&{第二腿}` | 服务 → 客户端 | 1 | 是 | 四向跨期价差推送 |
| `tq/sub/{user}` | 客户端 → 服务 | 1 | 否 | 增加 / 订阅 |
| `tq/unsub/{user}` | 客户端 → 服务 | 1 | 否 | 删除 / 退订 |
| `tq/status/{user}` | 服务 → 客户端 | 1 | 是 | 订阅处理结果回执 |

`{user}` 为服务方分配的用户名，全小写字母数字，不能包含 `/`、`+`、`#`。

## 2. 快速接入流程

1. 连接 broker（固定 `client_id` + `clean_session=false`，broker 会记住订阅并在离线期间补推 QoS 1 消息）
2. 订阅 `tq/spread/{user}/#` 与 `tq/status/{user}`
3. 向 `tq/sub/{user}` 发布订阅内容（支持合约对与品种两种形式，见 §4）
4. 从 `tq/status/{user}` 确认生效结果，随后持续接收 `tq/spread/{user}/…` 推送

Python（paho-mqtt 2.x）最小示例：

```python
import json
import paho.mqtt.client as mqtt

USER = "u1"  # 服务方分配

def on_connect(c, u, f, rc, props=None):
    c.subscribe(f"tq/spread/{USER}/#", qos=1)
    c.subscribe(f"tq/status/{USER}", qos=1)
    c.publish(f"tq/sub/{USER}", json.dumps({"pairs": ["ag"]}), qos=1)

def on_message(c, u, m):
    print(m.topic, m.payload.decode())

c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=f"{USER}-app-1", clean_session=False)
c.username_pw_set("<账号>", "<密码>")
c.on_connect, c.on_message = on_connect, on_message
c.connect("<broker地址>", 1883, keepalive=60)
c.loop_forever()
```

图形化调试推荐 [MQTTX](https://mqttx.app/)。

## 3. 价差推送（服务 → 客户端）

### 主题：`tq/spread/{user}/{第一腿}&{第二腿}`

每个生效的跨期组合一个独立主题，主题名即合约对。任一腿行情变动即发布；行情安静时每 5 秒（`MQTT_PUSH_INTERVAL`，服务端配置）重推一次最后快照；retain 更新，新订阅立得当前值。

payload 字段：

| 字段 | 类型 | 说明 | 示例 |
| --- | --- | --- | --- |
| `short_open` | number | 空开：卖 a 买 b = `a.bid − b.ask` | `9.0` |
| `long_open` | number | 多开：买 a 卖 b = `a.ask − b.bid` | `11.0` |
| `short_cover` | number | 空平：买 a 卖 b（恒等于 `long_open`） | `11.0` |
| `long_cover` | number | 多平：卖 a 买 b（恒等于 `short_open`） | `9.0` |

成功消息示例（主题 `tq/spread/u1/ag2702&ag2612`）：

```json
{"short_open": 9.0, "long_open": 11.0, "short_cover": 11.0, "long_cover": 9.0}
```

约定与边界：

- `a` = 第一腿（远月），`b` = 第二腿（近月）；价格均为买一/卖一价，保留 2 位小数。
- 合约对主题的腿序由订阅时的条目决定；**品种自动展开的组合固定交割月较晚者为 a、较早者为 b**。
- 价格无效（0/NaN，如未开盘、涨跌停无对手价）时不发布该对，待有效价格出现后恢复。
- 推送不含行情时间戳字段；客户端可用「值是否变化」判断行情是否更新。

## 4. 订阅控制（客户端 → 服务）

### 4.1 主题：`tq/sub/{user}`（增加 / 订阅）

payload：

| 字段 | 位置 | 类型 | 必填 | 说明 | 示例 |
| --- | --- | --- | --- | --- | --- |
| `pairs` | body | string 或 string[] | 是 | 订阅条目，单个字符串或数组；条目支持「合约对」与「品种」两种形式 | `["ag", "cu2608&cu2606"]` |

条目形式与校验规则：

| 形式 | 格式 | 校验规则 | 展开行为 |
| --- | --- | --- | --- |
| 合约对 | `第一腿&第二腿`，如 `ag2612&ag2610` | 两侧均为「字母+数字」且两腿不同 | 原样作为一个生效对，a=第一腿、b=第二腿 |
| 品种 | 纯字母，如 `ag` | 该品种在市合约数 ≥ 1，否则列入 `invalid` | 展开为该品种**全部在市合约**的跨期组合：C(N,2) 个，交割月较晚者为 a |

语义：

- **增量生效**：不影响已有订阅；重复提交同一对/品种幂等（不重发）。
- 新增生效对在处理时**立即发布一次当前价差**（价格无效则进入 `pending`，行情到达后自动开始推送）。
- 品种在订阅时与服务端重启时按当时在市合约展开；新合约上市/摘牌后，重启服务或重新提交该品种即跟随。

请求示例：

```json
{"pairs": "ag"}
```

```json
{"pairs": ["ag2612&ag2610", "cu2608&cu2606"]}
```

```json
{"pairs": ["ag", "cu2608&cu2606"]}
```

### 4.2 主题：`tq/unsub/{user}`（删除 / 退订）

payload 与 §4.1 完全一致，语义为移除：

- 删除合约对 → 移除该对；
- 删除品种 → 移除该品种展开的全部组合；
- 被移除的合约对在 broker 上的 retain 价差同步清除（新订阅者不会再收到）；
- 幂等：删除不存在的条目无副作用。

### 4.3 控制消息通用规则

- **不要设置 retain**：服务端忽略 retained 控制消息（防历史消息重放）；此前若发过 retained 控制消息，向原主题发空 payload 即可清除。
- 空 payload（用于清除 retain）会被静默忽略，不产生状态回执。
- `{user}` 包含 `/`、`+`、`#` 时消息被忽略。

## 5. 订阅结果（服务 → 客户端）

### 主题：`tq/status/{user}`

每次控制消息处理完成后发布（retain，新订阅即得最后结果）。

payload 字段：

| 字段 | 类型 | 说明 | 示例 |
| --- | --- | --- | --- |
| `subscribed` | string[] | 当前全部生效合约对（含品种展开），`第一腿&第二腿` | `["ag2702&ag2612", "ag2702&ag2610", "ag2612&ag2610"]` |
| `products` | string[] | 当前订阅的品种 | `["ag"]` |
| `invalid` | array | 格式错误或无在市合约的原始条目 | `["ag&", "xx"]` |
| `pending` | string[] | 已生效但行情尚未就绪的合约对（行情到达后自动推送） | `["ag2702&ag2612"]` |
| `error` | string | 仅在 payload 无法解析时出现 | `"无效 JSON"` |

成功示例：

```json
{"subscribed": ["ag2702&ag2612", "ag2702&ag2610", "ag2612&ag2610"], "products": ["ag"], "invalid": [], "pending": []}
```

## 6. 错误与常见问题

| 现象 | 原因与处理 |
| --- | --- |
| `status.error: "无效 JSON"` | 控制消息不是合法 JSON |
| `status.error: 格式应为…` | payload 缺少 `pairs` 字段或类型不对 |
| 条目出现在 `invalid` | 格式错误（如两腿相同、缺数字、非字符串），或品种当前无在市合约 |
| 条目/组合出现在 `pending` | 已订阅并持久化，但行情尚未就绪，到达后自动推送，无需重发 |
| 连接被 broker 断开 | 账号密码错误，或违反 ACL 权限 |
| 订阅成功但收不到推送 | 检查是否订阅了 `tq/spread/{user}/#`；`pending` 中的对要等行情；非交易时段无推送 |

## 7. 内部数据通道（默认不对接）

以下为服务内部存储与通道，供运维排查使用，第三方接入不需要：

| 名称 | 说明 |
| --- | --- |
| Redis Hash `SHFE:{品种}:{合约}` | 原始关键行情（datetime、last_price、买卖一价/量、volume、open_interest） |
| Redis Pub/Sub `shfe:quotes` | 全合约行情增量推送（服务内部消费） |
| Redis Set `tq:subs:{user}` | 用户订阅注册表（服务端持久化，勿手改） |

如需直接消费 Redis 原始行情，另行与服务方确认连接方式与权限。

## 8. 待确认事项

- **连接信息**：broker 实际地址、端口，以及各用户的 MQTT 账号密码，由服务方线下提供（本文档使用占位符）。
- **TLS**：当前 broker 未启用 TLS；如需 8883 端口加密接入请与服务方确认开通状态与证书要求。
- **持久会话保留时长**：客户端 `clean_session=false` 的会话（含离线消息）在 broker 上的保留策略由 Mosquitto 配置决定，需服务方确认。
