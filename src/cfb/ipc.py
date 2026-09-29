"""CFB 唯一容器间协议；业务类型继续复用原 Operation/Reply。"""

import asyncio
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .models import Operation

MAX_REQUEST = 64 * 1024
MAX_RESPONSE = 8 * 1024 * 1024
Kind = Literal["execute", "status", "health", "pause", "resume", "clean", "screenshot"]


class Request(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)
    version: Literal[1] = 1
    kind: Kind
    request_id: str = Field(min_length=1, max_length=128, pattern=r"^[!-~]+$")
    operation: Operation | None = None
    configuration: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[!-~]+$")

    @model_validator(mode="after")
    def payload_matches_kind(self):
        if (self.kind == "execute") != (self.operation is not None):
            raise ValueError("execute requires operation")
        if self.kind == "execute" and self.configuration is None:
            raise ValueError("execute requires configuration identity")
        if self.kind != "execute" and self.idempotency_key is not None:
            raise ValueError("control cannot carry an idempotency key")
        return self


class Response(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)
    version: Literal[1] = 1
    status: int = Field(ge=100, le=599)
    body: JsonValue
    headers: dict[str, str] = Field(default_factory=dict)


async def read_frame(reader: asyncio.StreamReader, limit: int) -> bytes:
    try:
        data = await reader.readuntil(b"\n")
    except (asyncio.LimitOverrunError, asyncio.IncompleteReadError):
        raise ValueError("invalid or incomplete CFB frame") from None
    if len(data) > limit:
        raise ValueError("CFB frame exceeds limit")
    return data


async def write_frame(writer: asyncio.StreamWriter, value: BaseModel, limit: int) -> None:
    data = value.model_dump_json().encode() + b"\n"
    if len(data) > limit:
        raise ValueError("CFB frame exceeds limit")
    writer.write(data)
    await writer.drain()
