"""公共时间接口保留上游字段，时间单位明确为 Unix 毫秒。"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class PublicTimeResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    serverTime: int = Field(
        strict=True,
        ge=0,
        le=2**63 - 1,
        description="币安返回的 Unix 时间戳，单位毫秒；原值转发，未补偿网络延迟。",
        examples=[1789689600123],
        json_schema_extra={"format": "int64"},
    )


class PublicTimeErrorDetail(BaseModel):
    code: Literal[
        "PUBLIC_TIME_TIMEOUT",
        "PUBLIC_TIME_NETWORK_ERROR",
        "PUBLIC_TIME_UPSTREAM_ERROR",
        "PUBLIC_TIME_INVALID_RESPONSE",
    ] = Field(description="公共时间请求的失败原因。")
    upstream_status: int | None = Field(
        default=None,
        description="上游返回非成功 HTTP 状态时提供其状态码，例如 429 或 451。",
    )


class PublicTimeErrorResponse(BaseModel):
    detail: PublicTimeErrorDetail
