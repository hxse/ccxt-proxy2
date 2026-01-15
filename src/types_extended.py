from pydantic import BaseModel
from typing import Literal
from src.types import ExchangeName, MarketType, ModeType


class FetchOpenOrdersRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str | None = None
    since: int | None = None
    limit: int | None = None


class FetchClosedOrdersRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str | None = None
    since: int | None = None
    limit: int | None = None


class FetchMyTradesRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str | None = None
    since: int | None = None
    limit: int | None = None


class FetchPositionsRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbols: list[str] | None = None


class SetLeverageRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    leverage: int
    symbol: str | None = None
    model_config = {"extra": "allow"}


class SetMarginModeRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    marginMode: Literal["cross", "isolated"]
    symbol: str | None = None
    model_config = {"extra": "allow"}


class CancelOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    id: str
    symbol: str | None = None
    model_config = {"extra": "allow"}
