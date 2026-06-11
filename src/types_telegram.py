from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


TelegramParseMode = Literal["MarkdownV2", "HTML"]


class TelegramSendMessageRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chats: list[str] = Field(
        ...,
        min_length=1,
        title="目标 chat alias 列表",
        description=(
            "目标 chat alias 数组。每个 alias 必须存在于服务端配置 "
            "telegram.chats 中；服务端会 strip 后校验非空和不重复。"
            "请求体不能传裸 chat_id。"
        ),
        examples=[["scanner", "ops"]],
    )
    text: str = Field(
        ...,
        min_length=1,
        max_length=4096,
        title="消息文本",
        description="发送给 Telegram 的文本。最大 4096 字符，不能是纯空白文本。",
        examples=["ccxt-proxy2 notification"],
    )
    parse_mode: TelegramParseMode | None = Field(
        None,
        title="Telegram parse_mode",
        description="可选。透传 Telegram parse_mode，仅支持 MarkdownV2 或 HTML。",
        examples=["HTML"],
    )
    disable_web_page_preview: bool = Field(
        True,
        title="是否禁用链接预览",
        description="透传 Telegram disable_web_page_preview。默认 true。",
    )
    disable_notification: bool = Field(
        False,
        title="是否静默发送",
        description="透传 Telegram disable_notification。默认 false。",
    )

    @field_validator("chats")
    @classmethod
    def validate_chats(cls, chats: list[str]) -> list[str]:
        normalized = [chat.strip() for chat in chats]
        if any(not chat for chat in normalized):
            raise ValueError("telegram chats must not contain empty aliases")
        if len(set(normalized)) != len(normalized):
            raise ValueError("telegram chats must not contain duplicate aliases")
        return normalized

    @field_validator("text")
    @classmethod
    def validate_text(cls, text: str) -> str:
        if not text.strip():
            raise ValueError("telegram text must not be blank")
        return text
