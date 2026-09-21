# Telegram 文本消息转发

`POST /telegram/send_message` 通过同一个 Telegram Bot 向一个或多个预配置 chat 发送文本消息，复用本项目 Bearer 鉴权。调用方传入最终文本和目标 alias，服务不绑定 scanner 消息格式，不接受请求体中的裸 chat_id。

## 能力与配置

当前只转发 Telegram Bot API sendMessage，支持 text、parse_mode、disable_web_page_preview 和 disable_notification。目标 chat_id 从服务端配置解析。媒体、编辑或删除消息、inline keyboard、webhook 和每个 chat 独立 token 均不属于当前接口。

```toml
[telegram]
bot_token = "YOUR_BOT_TOKEN"

[telegram.chats]
scanner = "-1001111111111"
ops = "-1002222222222"
debug = "123456789"
```

配置只接受 bot_token 与非空 chats 映射，没有 default_chat 或单个 chat_id 字段。Bot token 和 chat_id 去除首尾空白后不得为空，alias 必须匹配 `^[A-Za-z0-9_-]+$`。每个请求显式指定目标，不能隐式选择默认群组。

配置只在启动时读取，公共规则见[配置规范](configuration.md)。Telegram 不使用 service_whitelist；它按请求创建并复用 HTTP 客户端，应用退出时关闭自己持有的客户端。未配置 Telegram 的请求返回明确错误。

## 请求契约

HTTP 参数只放在 JSON body 中，未知字段和 query 参数被拒绝。示例：

```json
{
  "chats": ["scanner", "ops"],
  "text": "hello",
  "parse_mode": null,
  "disable_web_page_preview": true,
  "disable_notification": false
}
```

| 参数 | 类型 | 默认值 | 约束 |
| --- | --- | --- | --- |
| chats | list[str] | 必填 | 至少一个已配置 alias；strip 后非空、不重复 |
| text | str | 必填 | 1～4096 字符，不允许纯空白文本；保留实际文本 |
| parse_mode | MarkdownV2、HTML 或 null | null | 原样传给 Telegram |
| disable_web_page_preview | bool | true | 禁用链接预览 |
| disable_notification | bool | false | 是否静默发送 |

所有 alias 先检查完毕，发现未知目标即返回 422，不开始发送；错误中的 unknown_chats 列出未配置的 alias。请求模型为 [TelegramSendMessageRequest](../../src/types_telegram.py)。

## 发送与客户端生命周期

[TelegramManager](../../src/tools/telegram_manager.py)解析 alias、构造发送 URL，并按请求顺序逐目标发送。一个目标失败仍继续尝试其他目标，同一请求复用 HTTP 客户端，不提供事务回滚。

HTTP 客户端的超时为 10 秒。每个目标最多尝试 3 次，失败后的间隔为 3 秒；仅 ConnectError 和 ConnectTimeout 可重试。读超时、其他 HTTP 错误、上游非成功响应和业务拒绝均不重试，因为请求可能已经到达 Telegram。

这里的次数是“包含首次发送在内最多 3 次尝试”，不是首次发送后再重试 3 次。各目标独立计数，不能因一个目标的失败重新发送已经成功的目标。

上游返回 ok=true 且包含 result.message_id 才认定成功。返回非 JSON、ok=false 或缺少 message_id 均为失败。

应用退出时调用幂等 close；Manager 只关闭自己创建的客户端，不关闭外部注入客户端。httpx 属于运行时依赖。

## 成功与局部失败

全部目标成功时返回 HTTP 200：

```json
{
  "items": [
    {
      "chat": "scanner",
      "chat_id": "-1001111111111",
      "ok": true,
      "message_id": 123,
      "error": null
    },
    {
      "chat": "ops",
      "chat_id": "-1002222222222",
      "ok": true,
      "message_id": 456,
      "error": null
    }
  ]
}
```

每项包含 chat、chat_id、ok、message_id 和 error。成功项必须有 message_id 且 error=null；失败项必须有非空 error 且 message_id=null。完整类型见 [responses_telegram.py](../../src/responses_telegram.py)。

任意目标失败时返回 502，保留所有目标的结果：

```json
{
  "detail": {
    "code": "TELEGRAM_SEND_FAILED",
    "items": [
      {
        "chat": "scanner",
        "chat_id": "-1001111111111",
        "ok": true,
        "message_id": 123,
        "error": null
      },
      {
        "chat": "ops",
        "chat_id": "-1002222222222",
        "ok": false,
        "message_id": null,
        "error": "Bad Request: chat not found"
      }
    ]
  }
}
```

已成功发送的消息继续有效，HTTP 502 不表示所有目标都未收到消息。

## 错误与信息保护

| 情况 | HTTP | 响应 |
| --- | --- | --- |
| 未通过本项目鉴权 | 401 | 既有鉴权错误 |
| 参数缺失、空白、重复或未知字段 | 422 | FastAPI/Pydantic 参数错误 |
| 未配置的 alias | 422 | detail.code=TELEGRAM_UNKNOWN_CHAT，附 unknown_chats |
| 没有 Telegram 配置 | 500 | detail="TELEGRAM_NOT_CONFIGURED" |
| 至少一个目标发送失败 | 502 | detail.code=TELEGRAM_SEND_FAILED，附全部 items |

已经声明但不完整的 Telegram 配置在加载阶段报错。Bot token 不进入 HTTP 响应和日志；发送 URL 脱敏为 `https://api.telegram.org/bot***/sendMessage`，上游错误说明也必须避免携带 token。真实 token 不进入测试数据、文档或版本控制。

## 验证与手动入口

默认离线测试使用 mock transport/manager，不访问 Telegram。应覆盖配置、参数与 alias 校验、单目标与多目标成功、局部失败、非法上游响应、缺少 message_id、仅连接失败重试，以及 token 脱敏。成功和失败项均核对完整状态关系。

真实发送具有外部副作用，只通过显式入口运行，不进入普通 online suite：

```text
just test-telegram-offline
just debug-telegram-stateful
just debug-telegram-send scanner "hello"
```

有状态探针位于 [debug/test_telegram_stateful.py](../../debug/test_telegram_stateful.py)，通过 TELEGRAM_TEST_CHAT 可选定测试 alias，默认使用首个已配置目标；TELEGRAM_TEST_TEXT 可指定文本。该探针直接调用 Manager，HTTP 鉴权由离线路由测试和手动 HTTP 请求另行覆盖。

Bruno 示例为 [TELEGRAM/send_message/main.bru](../../bruno/TELEGRAM/send_message/main.bru)，复用 collection 登录，名称标记 [STATEFUL]。Bruno 与离线、在线和调试入口的共同边界见[验证规范](verification.md)。
