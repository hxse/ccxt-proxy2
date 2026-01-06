from pydantic import BaseModel
from typing import Optional, Literal
from pathlib import Path


VALID_PERIODS = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]


class FileInfo(BaseModel):
    """缓存文件信息模型"""

    symbol: str
    period: str
    start_time: int
    end_time: int
    count: int


class OHLCVParams(BaseModel):
    """OHLCV 请求参数模型"""

    exchange_name: str
    market: Literal["future", "spot"]
    symbol: str
    period: VALID_PERIODS
    start_time: Optional[int] = None
    count: Optional[int] = None
    enable_cache: bool = True
    enable_test: bool = False
    cache_dir: Path = Path("./data")
    sandbox: bool = False


class MarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    is_usd_amount: bool = False
    sandbox: bool = True


class LimitOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    price: float
    is_usd_amount: bool = False
    sandbox: bool = True


class StopMarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    reduceOnly: bool = True
    stopLossPrice: float | None = None
    is_usd_amount: bool = False
    sandbox: bool = True


class TakeProfitMarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    reduceOnly: bool = True
    takeProfitPrice: float | None = None
    is_usd_amount: bool = False
    sandbox: bool = True


class StopMarketOrderPercentageRequest(BaseModel):
    amount_percentage: float = 1
    exchange_name: str
    symbol: str
    side: str
    reduceOnly: bool = True
    stopLossPrice: float | None = None
    # 不完全确定contracts的参数类型,所以这里的is_usd_amount最好保持false,避免未知情况
    is_usd_amount: bool = False
    sandbox: bool = True


class TakeProfitMarketOrderPercentageRequest(BaseModel):
    amount_percentage: float = 1
    exchange_name: str
    symbol: str
    side: str
    reduceOnly: bool = True
    takeProfitPrice: float | None = None
    # 不完全确定contracts的参数类型,所以这里的is_usd_amount最好保持false,避免未知情况
    is_usd_amount: bool = False
    sandbox: bool = True


class CloseAllOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    sandbox: bool = True


class CancelAllOrdersRequest(BaseModel):
    exchange_name: str
    symbol: str
    sandbox: bool = True


class BalanceRequest(BaseModel):
    """获取余额请求参数"""

    exchange_name: str
    sandbox: bool = True


class TickersRequest(BaseModel):
    """获取报价请求参数"""

    exchange_name: str
    symbols: str | None = None
    sandbox: bool = False
