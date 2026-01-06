from pydantic import BaseModel
from typing import Optional, Literal


VALID_PERIODS = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
OrderType = Literal["market", "limit", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
SideType = Literal["buy", "sell"]
ExchangeName = Literal["binance", "kraken"]
MarketType = Literal["future", "spot"]
ModeType = Literal["sandbox", "live"]


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


class OHLCVParams(BaseModel):
    """OHLCV 请求参数模型"""

    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    period: VALID_PERIODS
    start_time: Optional[int] = None
    count: Optional[int] = None
    enable_cache: bool = True
    enable_test: bool = False


class MarketOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    amount: float
    is_usd_amount: bool = False


class LimitOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    amount: float
    price: float
    is_usd_amount: bool = False


class StopMarketOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    amount: float
    reduceOnly: bool = True
    stopLossPrice: float | None = None
    is_usd_amount: bool = False


class TakeProfitMarketOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    amount: float
    reduceOnly: bool = True
    takeProfitPrice: float | None = None
    is_usd_amount: bool = False


class StopMarketOrderPercentageRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    reduceOnly: bool = True
    stopLossPrice: float | None = None
    # 不完全确定contracts的参数类型,所以这里的is_usd_amount最好保持false,避免未知情况
    is_usd_amount: bool = False
    amount_percentage: float = 1


class TakeProfitMarketOrderPercentageRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str
    side: SideType
    reduceOnly: bool = True
    takeProfitPrice: float | None = None
    # 不完全确定contracts的参数类型,所以这里的is_usd_amount最好保持false,避免未知情况
    is_usd_amount: bool = False
    amount_percentage: float = 1


class CloseAllOrderRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str


class CancelAllOrdersRequest(BaseModel):
    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbol: str


class BalanceRequest(BaseModel):
    """获取余额请求参数"""

    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"


class TickersRequest(BaseModel):
    """获取报价请求参数"""

    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"
    symbols: str | None = None
