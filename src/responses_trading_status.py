"""TQ / CTP 共用的交易状态语义；未知状态不能转换为休市。"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

TradingStatusReason = Literal[
    "not_received", "disconnected", "timeout", "unavailable", "unrecognized_status"
]


class TradingStatusResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    is_open: bool | None = Field(
        None,
        description="true=连续交易；false=明确处于其他阶段（含集合竞价）；null=无法确认。不是交易日判断，也不保证订单会被接受。",
        examples=[True, False, None],
    )
    raw_status: str | None = Field(
        None,
        description="当前有效连接收到的上游原始状态；未收到、断线或超时时为 null，不返回旧连接状态。",
    )
    reason: TradingStatusReason | None = Field(
        None,
        description="已确认时为 null；not_received=尚未收到，disconnected=断线，timeout=等待超时，unavailable=服务不可用，unrecognized_status=未知状态编码。",
    )
