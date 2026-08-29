# Telegram 消息转发设计蓝图

> **状态：Implemented。** 本文描述当前 Telegram route contract；它不属于 CCXT/DuckDB 重构范围。

## 目标

本设计为 `ccxt-proxy2` 增加一个独立的 Telegram 消息转发层，用于通过同一个 Telegram Bot 向多个预配置 chat 发送消息。

第一版只提供通用文本消息转发能力：

- 一个 `bot_token` 管理多个 chat。
- chat 在配置文件中使用稳定 alias 管理。
- HTTP 请求必须显式选择发送目标。
- 路由复用本项目现有 `/auth/token` Bearer token 鉴权机制。
- 路由不绑定 scanner 信号格式，调用方直接传入最终消息文本。
- 服务端不接受任意裸 `chat_id` 作为发送目标，只允许选择配置中的 chat alias。

提供一个路由：

- `/telegram/send_message`

## 现有 Telegram 发送模型

参考实现位于：

- `/home/hxse/dev/pyo3-quant/py_entry/scanner/notifier.py`
- `/home/hxse/dev/pyo3-quant/py_entry/scanner/config.py`
- `/home/hxse/dev/pyo3-quant/py_entry/scanner/main.py`

现有模型是单 bot、单 chat：

```python
Notifier(token=config.telegram_bot_token, chat_id=config.telegram_chat_id)
```

发送逻辑是对 Telegram Bot API 的 `sendMessage` 做薄包装：

```python
POST https://api.telegram.org/bot{token}/sendMessage
{
  "chat_id": "...",
  "text": "..."
}
```

该实现包含：

- `httpx.Client(timeout=10.0)`
- 最多 3 次重试
- 每次失败后等待 3 秒
- 只用于 scanner 内部格式化后的信号报告

本项目新增能力应保留它的核心发送语义，但不继承单 chat 限制，也不继承 scanner 的消息格式。

## 能力边界

第一版只支持 Telegram Bot API `sendMessage`：

```text
POST /bot{bot_token}/sendMessage
```

支持参数：

- `chat_id`
- `text`
- `parse_mode`
- `disable_web_page_preview`
- `disable_notification`

第一版不支持：

- 图片、文件、音频、视频等媒体发送。
- 编辑历史消息。
- 删除消息。
- reply markup / inline keyboard。
- webhook 接收。
- 每个 chat 使用不同 bot token。
- 请求体中直接传裸 `chat_id` 绕过配置。

## 配置设计

配置模型新增 `telegram`：

```json
{
  "telegram": {
    "bot_token": "...",
    "chats": {
      "scanner": "-1001111111111",
      "ops": "-1002222222222",
      "debug": "123456789"
    }
  }
}
```

字段说明：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `bot_token` | `str` | 是 | Telegram Bot Token |
| `chats` | `dict[str, str]` | 是 | chat alias 到 Telegram `chat_id` 的映射 |

不设计 `default_chat`。发送目标必须由每次请求显式提供，避免隐式发送到错误群组。

配置只支持 `bot_token + chats` 一种结构；单个 `chat_id` 字段不属于配置模型。

推荐 Pydantic 模型：

```python
class TelegramConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bot_token: str = Field(..., min_length=1)
    chats: dict[str, str] = Field(..., min_length=1)
```

配置校验规则：

- `bot_token` 不能为空。
- `chats` 不能为空。
- chat alias 不能为空字符串。
- chat id 不能为空字符串。
- chat alias 不应包含空白字符，推荐使用 `[a-zA-Z0-9_-]+`。

## 路由设计

### `POST /telegram/send_message`

用途：向一个或多个预配置 Telegram chat 发送文本消息。

鉴权：复用本项目现有 `Depends(manager)`，即调用方必须先通过 `/auth/token` 获取 Bearer token。

请求体：

```json
{
  "chats": ["scanner", "ops"],
  "text": "hello",
  "parse_mode": null,
  "disable_web_page_preview": true,
  "disable_notification": false
}
```

请求参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `chats` | `list[str]` | 是 | 无 | 目标 chat alias 列表，必须存在于配置 `telegram.chats` 中；strip 后不允许重复 |
| `text` | `str` | 是 | 无 | 要发送的文本，按 Telegram `sendMessage` 限制最大 4096 字符；不允许纯空白文本 |
| `parse_mode` | `Literal["MarkdownV2", "HTML"] \| None` | 否 | `null` | 透传 Telegram `parse_mode` |
| `disable_web_page_preview` | `bool` | 否 | `true` | 透传 Telegram `disable_web_page_preview` |
| `disable_notification` | `bool` | 否 | `false` | 透传 Telegram `disable_notification` |

推荐请求模型：

```python
class TelegramSendMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chats: list[str] = Field(..., min_length=1)
    text: str = Field(..., min_length=1, max_length=4096)
    parse_mode: Literal["MarkdownV2", "HTML"] | None = None
    disable_web_page_preview: bool = True
    disable_notification: bool = False
```

响应：

```json
{
  "items": [
    {
      "chat": "scanner",
      "chat_id": "-1001111111111",
      "ok": true,
      "message_id": 123
    },
    {
      "chat": "ops",
      "chat_id": "-1002222222222",
      "ok": true,
      "message_id": 456
    }
  ]
}
```

推荐响应模型：

```python
class TelegramSendMessageItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chat: str
    chat_id: str
    ok: bool
    message_id: int | None = None
    error: str | None = None


class TelegramSendMessageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[TelegramSendMessageItem]
```

路由 docstring 或 `description` 应说明：

```text
本路由是 Telegram Bot API sendMessage 薄转发。一个 bot_token 可配置多个 chat，调用方必须通过 chats 显式选择目标 chat alias。服务端不会接受请求体中的裸 chat_id。
```

## 错误语义

### 配置错误

缺少 `telegram` 配置：

```http
500 Internal Server Error
```

错误码建议：

```json
{
  "detail": "TELEGRAM_NOT_CONFIGURED"
}
```

缺少 `telegram.bot_token` 或 `telegram.chats` 为空，应在配置加载阶段失败，避免服务以不可用状态启动。

### 请求错误

`chats` 缺失或为空：

```http
422 Unprocessable Entity
```

FastAPI/Pydantic 可直接处理。

`chats` strip 后存在重复 alias、包含空 alias，或 `text` 是纯空白文本：

```http
422 Unprocessable Entity
```

chat alias 不存在：

```http
422 Unprocessable Entity
```

错误示例：

```json
{
  "detail": {
    "code": "TELEGRAM_UNKNOWN_CHAT",
    "unknown_chats": ["foo"]
  }
}
```

### 发送错误

Telegram API 网络失败、超时、返回非 2xx、或响应 JSON 中 `ok != true`：

```http
502 Bad Gateway
```

多 chat 发送时建议先尝试全部目标，再根据结果决定 HTTP 状态：

- 全部成功：`200`
- 任意失败：`502`

失败响应应包含每个 chat 的发送结果，方便调用方判断局部失败：

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

## 发送器设计

新增独立工具模块：

```text
src/tools/telegram_manager.py
```

职责：

- 读取 `app_config.telegram`。
- 解析 chat alias 到真实 `chat_id`。
- 构造 Telegram Bot API URL。
- 持有 `httpx.Client(timeout=10.0)`。
- 对连接建立失败或连接超时执行最多 3 次重试。
- 解析 Telegram API 响应。
- 返回结构化发送结果。

建议行为：

- token 只用于构造请求 URL，不进入日志和异常消息。
- Telegram 原始错误描述可以保留，但需要避免包含 bot token。
- 发送同一请求中的多个 chat 时复用同一个 HTTP client。
- 每个 chat 独立重试，避免一个 chat 的临时连接失败影响其他 chat 的首次发送。
- `sendMessage` 不是幂等接口。若本地发生读超时，服务端无法判断 Telegram 是否已经收到请求，因此不重试读超时，避免重复投递。
- Telegram 返回 `ok=true` 但缺少 `result.message_id` 时视为失败。

依赖说明：

Telegram 路由运行时需要 `httpx`，因此 `httpx` 必须声明为项目运行时依赖，而不是仅放在 dev dependency。

## 目录结构

建议新增：

```text
src/types_telegram.py
src/responses_telegram.py
src/tools/telegram_manager.py
src/router/telegram_router.py
Test/test_telegram_router.py
Test/test_telegram_manager.py
Test/online/test_telegram_online.py
```

需要修改：

```text
src/tools/config_types.py
src/main.py
pyproject.toml
justfile
bruno/
```

## 测试策略

测试分为离线自动化测试和在线手动测试。

### 离线测试

离线测试必须进入默认 `just test`，不得真实访问 Telegram。

覆盖点：

- 配置模型接受 `telegram.bot_token + telegram.chats`。
- 空 `chats` 配置加载失败。
- 请求体 `chats` 必填且不能为空。
- 请求体 `chats` strip 后重复时返回 422。
- 请求体 `text` 是纯空白文本时返回 422。
- 未知 chat alias 返回 `422`。
- 单 chat 成功发送。
- 多 chat 成功发送。
- 多 chat 局部失败时返回 `502`，并包含每个 chat 的结果。
- Telegram API 返回 `ok=false` 时视为失败。
- Telegram API 返回 `ok=true` 但缺少 `result.message_id` 时视为失败。
- Telegram API 返回非 JSON 时视为失败。
- 连接失败可以重试，读超时不重试，避免重复投递。
- 日志和错误信息不泄露 bot token。

HTTP 层可使用 FastAPI `TestClient`。Telegram 外部请求使用 mock transport 或 mock manager。

### 在线测试

在线测试放在 `Test/online`，默认 `just test` 不收集。

建议入口：

```text
just test-telegram-online
```

在线测试前置条件：

- `data/config.json` 中配置真实 `telegram.bot_token`。
- `telegram.chats` 中存在测试 chat alias。
- 执行命令显式传入或使用环境变量指定测试 alias。

在线测试只验证最小闭环：

- 通过本项目鉴权。
- 调用 `/telegram/send_message`。
- Telegram API 返回成功。
- 响应中 `ok=true` 且包含 `message_id`。

### Bruno

新增 Bruno 请求：

```text
bruno/TELEGRAM/Send Message.bru
```

用途：

- 手动调用 `/telegram/send_message`。
- 使用现有 auth token 流程。
- 请求体中的 `chats` 使用环境变量或示例 alias。

所有自动化或手动测试命令都应通过 `just` 入口运行。

## 实施步骤

1. 新增 `TelegramConfig`，并挂到 `AppConfig.telegram`。
2. 将 `httpx` 提升为运行时依赖。
3. 新增 Telegram 请求与响应 Pydantic 类型。
4. 新增 `telegram_manager`，封装 chat alias 解析、HTTP 发送、重试和错误转换。
5. 新增 `telegram_router`，接入现有 Bearer token 鉴权。
6. 在 `src/main.py` 注册路由。
7. 增加离线测试。
8. 增加在线手动测试入口。
9. 增加 Bruno 请求。
10. 运行 `just check` 和 `just test`。

## 安全约束

- 不在日志中输出 bot token。
- 不在响应中输出 bot token。
- 不允许请求体传任意裸 `chat_id`。
- 只允许发送到配置中声明的 chat alias。
- 路由必须使用本项目现有 token 鉴权。
- 配置文件中的真实 token 不进入测试快照、文档示例或提交内容。

## 后续扩展

后续可以在不破坏第一版接口的前提下增加：

- `POST /telegram/send_photo`
- `POST /telegram/send_document`
- `reply_to_message_id`
- `message_thread_id`
- per-chat 限流
- per-chat enabled 开关
- 发送审计日志

这些扩展不应改变 `send_message` 的核心约束：目标 chat 必须来自服务端配置，不能由请求体传入任意裸 `chat_id`。
