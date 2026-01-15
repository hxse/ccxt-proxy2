from pydantic import BaseModel
from typing import Optional, Annotated
from fastapi import Query
from src.base_types import (
    ExchangeName,
    MarketType,
    ModeType,
    SideType,
    PositionSide,
    VALID_PERIODS,
    BaseExchangeRequest,
    BaseSymbolRequest,
)


class ExchangeWhitelistItem(BaseModel):
    exchange: ExchangeName
    market: MarketType
    mode: ModeType


class FileInfo(BaseModel):
    """缓存文件信息模型"""

    symbol: str
    period: str
    start_time: int
    end_time: int
    count: int


class OHLCVParams(BaseSymbolRequest):
    """OHLCV 请求参数模型"""

    timeframe: VALID_PERIODS
    since: Optional[int] = None
    limit: Optional[int] = None
    enable_cache: bool = True
    enable_test: bool = False


class MarketOrderRequest(BaseSymbolRequest):
    side: SideType
    amount: float
    clientOrderId: str | None = None
    model_config = {"extra": "allow"}


class LimitOrderRequest(BaseSymbolRequest):
    side: SideType
    amount: float
    price: float
    clientOrderId: str | None = None
    timeInForce: str | None = None
    postOnly: bool = False
    model_config = {"extra": "allow"}


class StopMarketOrderRequest(BaseSymbolRequest):
    side: SideType
    amount: float
    reduceOnly: bool = True
    triggerPrice: float | None = None
    clientOrderId: str | None = None
    timeInForce: str | None = None
    model_config = {"extra": "allow"}


class TakeProfitMarketOrderRequest(BaseSymbolRequest):
    side: SideType
    amount: float
    reduceOnly: bool = True
    triggerPrice: float | None = None
    clientOrderId: str | None = None
    timeInForce: str | None = None
    model_config = {"extra": "allow"}


class ClosePositionRequest(BaseSymbolRequest):
    side: PositionSide | None = None
    model_config = {"extra": "allow"}


class CancelAllOrdersRequest(BaseExchangeRequest):
    symbol: str | None = None
    model_config = {"extra": "allow"}


class BalanceRequest(BaseExchangeRequest):
    """获取余额请求参数"""

    pass


class TickersRequest(BaseExchangeRequest):
    """获取报价请求参数"""

    symbols: Annotated[
        str | None,
        Query(
            default=None,
            description="交易对列表，多个用逗号分隔，例如：BTC/USDT,ETH/USDT",
            examples=["BTC/USDT", "BTC/USDT,ETH/USDT"],
        ),
    ]

    @property
    def symbols_list(self) -> list[str] | None:
        """将逗号分隔的 symbols 字符串转换为列表"""
        if not self.symbols or not isinstance(self.symbols, str):
            return None
        return [s.strip() for s in self.symbols.split(",") if s.strip()]


class MarketInfoRequest(BaseSymbolRequest):
    """获取市场信息请求参数"""

    pass


class FetchOrderRequest(BaseExchangeRequest):
    """获取特定订单请求参数"""

    symbol: str | None = None
    id: str


# MarketInfoResponse has been moved to src/responses.py
