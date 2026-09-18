from datetime import date as Date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from src.responses_trading_status import TradingStatusResponse


class TqTradingStatusResponse(TradingStatusResponse):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "symbol": "SHFE.rb2610",
                    "is_open": True,
                    "raw_status": "CONTINOUS",
                    "reason": None,
                },
                {
                    "symbol": "SHFE.rb2610",
                    "is_open": None,
                    "raw_status": None,
                    "reason": "not_received",
                },
            ]
        }
    )

    symbol: str = Field(
        description="本次查询的完整 TQ 合约代码。", examples=["SHFE.rb2610"]
    )
    raw_status: str | None = Field(
        None,
        description="TQ trade_status：CONTINOUS=连续交易（沿用上游拼写），AUCTIONORDERING=集合竞价报单，NOTRADING=非交易；未知编码原样保留。",
        examples=["CONTINOUS", "AUCTIONORDERING", "NOTRADING", None],
    )


TqRecord = dict[str, Any]


class TqTradingCalendarItem(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"date": "2026-09-11", "trading": True}]},
    )

    date: Date = Field(description="北京时间自然日")
    trading: bool = Field(description="该日期是否为中国期货交易日")


class TqUnderlyingItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    symbol: str = Field(..., min_length=1, title="TQ 主连 symbol")
    underlying_symbol: str = Field(..., min_length=1, title="当前标的合约")
    ins_class: str | None = Field(None, title="合约类型")
    exchange_id: str | None = Field(None, title="交易所")
    product_id: str | None = Field(None, title="品种")


class TqUnderlyingHistoryItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    date: str = Field(..., min_length=1, title="交易日")
    symbol: str = Field(..., min_length=1, title="TQ 主连 symbol")
    underlying_symbol: str = Field(..., min_length=1, title="当日标的合约")


class TqUnderlyingSymbolResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[TqUnderlyingItem] = Field(default_factory=list)
    history: list[TqUnderlyingHistoryItem] = Field(default_factory=list)
