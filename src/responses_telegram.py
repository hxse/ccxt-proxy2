from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TelegramSendMessageItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chat: str = Field(
        ...,
        min_length=1,
        title="chat alias",
        description="请求中传入的 chat alias，例如 scanner 或 ops。",
    )
    chat_id: str = Field(
        ...,
        min_length=1,
        title="Telegram chat_id",
        description="服务端配置中该 alias 对应的 Telegram chat_id。",
    )
    ok: bool = Field(
        ...,
        description="该 chat 的发送结果。true 表示 Telegram 返回 ok=true 且包含 message_id。",
    )
    message_id: int | None = Field(
        None,
        description="Telegram 返回的 result.message_id。ok=true 时必填，ok=false 时必须为空。",
    )
    error: str | None = Field(
        None,
        description="失败原因。ok=false 时必填，ok=true 时必须为空。",
    )

    @model_validator(mode="after")
    def validate_state(self) -> "TelegramSendMessageItem":
        if self.ok and self.message_id is None:
            raise ValueError("telegram successful send requires message_id")
        if self.ok and self.error is not None:
            raise ValueError("telegram successful send must not include error")
        if not self.ok and not self.error:
            raise ValueError("telegram failed send requires error")
        if not self.ok and self.message_id is not None:
            raise ValueError("telegram failed send must not include message_id")
        return self


class TelegramSendMessageResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[TelegramSendMessageItem] = Field(
        default_factory=list,
        description="每个目标 chat 的发送结果。200 响应中所有 item 都必须 ok=true。",
    )


class TelegramSendFailureDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: Literal["TELEGRAM_SEND_FAILED"] = "TELEGRAM_SEND_FAILED"
    items: list[TelegramSendMessageItem] = Field(
        ...,
        description="502 失败响应中的逐 chat 发送结果，包含已成功和失败的目标。",
    )
