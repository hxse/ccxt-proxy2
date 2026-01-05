from pydantic import BaseModel
from typing import Optional, Literal
from pathlib import Path
from decimal import Decimal


class OHLCVParams(BaseModel):
    """OHLCV 请求参数模型"""

    exchange_name: str
    symbol: str
    period: str
    start_time: Optional[int] = None
    count: Optional[int] = None
    enable_cache: bool = True
    enable_test: bool = False
    file_type: str = ".parquet"
    cache_size: int = 1000
    page_size: int = 1500
    cache_dir: Path = Path("./data")
    sandbox: bool = False


class FileInfo(BaseModel):
    """缓存文件信息模型"""

    symbol: str
    period: str
    start_time: int
    end_time: int
    count: int


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
