from typing import Any

from fastapi import APIRouter, Depends

from src.responses_telegram import TelegramSendMessageResponse
from src.router.auth_handler import manager
from src.tools.telegram_manager import telegram_manager
from src.types_telegram import TelegramSendMessageRequest

telegram_router = APIRouter(
    prefix="/telegram",
    dependencies=[Depends(manager)],
    tags=["TELEGRAM"],
)


TELEGRAM_SEND_MESSAGE_DESCRIPTION = """
通过服务端配置的 Telegram Bot 调用 Bot API `sendMessage`。

目标 chat 选择：

- 一个 `bot_token` 可以配置多个 chat。
- 请求体里的 `chats` 只能传配置中的 chat alias，例如 `scanner`、`ops`。
- 服务端不会接受裸 `chat_id` 字段；请求体包含 `chat_id` 会被 Pydantic 拒绝。
- `chats` 会先 strip，再校验非空和不重复；重复 alias 返回 422，避免同一条消息重复投递。

消息内容：

- `text` 必填，最大 4096 字符，并且不能是纯空白文本。
- `parse_mode` 只允许 `MarkdownV2` 或 `HTML`。
- 默认禁用网页预览，默认不静默发送。

发送与重试：

- 对每个 chat 独立发送，单个 chat 失败不阻止尝试其他 chat。
- `sendMessage` 不是幂等接口。服务端只重试连接建立失败或连接超时；读超时不重试，因为 Telegram 可能已经收到请求，重试可能导致重复消息。
- Telegram 返回 `ok=true` 但缺少 `result.message_id` 时视为失败。

响应：

- 全部 chat 成功时返回 200，`items` 中每项包含 `chat`、`chat_id`、`ok=true`、`message_id`。
- 任意 chat 失败时返回 502，`detail.code=TELEGRAM_SEND_FAILED`，`detail.items` 包含所有 chat 的成功/失败结果。
- 未配置 Telegram 返回 500 `TELEGRAM_NOT_CONFIGURED`。
- 未知 chat alias 返回 422 `TELEGRAM_UNKNOWN_CHAT`。

安全约束：

- 本路由复用本项目 Bearer token 鉴权。
- Bot token 不会出现在响应或日志中；Telegram Bot API URL 会脱敏为 `https://api.telegram.org/bot***/sendMessage`。
"""

TELEGRAM_SEND_MESSAGE_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": (
            "全部目标 chat 发送成功。每个 item 的 ok 为 true，且 message_id 必填。"
        )
    },
    401: {"description": "未通过本项目 Bearer token 鉴权。"},
    422: {
        "description": (
            "请求体校验失败或 chat alias 未配置。常见情况：chats 为空、"
            "chats strip 后重复、text 纯空白、请求体包含裸 chat_id、"
            "detail.code=TELEGRAM_UNKNOWN_CHAT。"
        )
    },
    500: {"description": "服务端未配置 Telegram，detail 为 TELEGRAM_NOT_CONFIGURED。"},
    502: {
        "description": (
            "Telegram API 调用失败或返回异常。detail.code 为 "
            "TELEGRAM_SEND_FAILED，detail.items 包含每个 chat 的发送结果。"
        )
    },
}


@telegram_router.post(
    "/send_message",
    response_model=TelegramSendMessageResponse,
    summary="Send Telegram message to configured chats",
    description=TELEGRAM_SEND_MESSAGE_DESCRIPTION,
    responses=TELEGRAM_SEND_MESSAGE_RESPONSES,
)
def send_message(params: TelegramSendMessageRequest) -> TelegramSendMessageResponse:
    """
    向一个或多个配置中的 Telegram chat alias 发送文本消息。
    """
    return telegram_manager.send_message(params)
