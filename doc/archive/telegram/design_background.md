# Telegram 早期设计背景

本文保留早期参考模型、实施顺序和未实施扩展，不作为当前功能承诺。现行接口、发送次数和错误规则见 [Telegram 规范](../../current_specs/telegram.md)。

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
