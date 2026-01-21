from pydantic import BaseModel, Field
from typing import Optional, Annotated, get_args
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

    timeframe: VALID_PERIODS = Field(
        ...,
        title="时间周期",
        description="K线周期 (Min: 1m, Max: 1M)",
        examples=list(get_args(VALID_PERIODS)),
    )
    since: Optional[int] = Field(
        None,
        title="起始时间戳 (ms)",
        description="获取该时间之后的数据",
        examples=[1672531200000],
    )
    limit: Optional[int] = Field(
        None, title="数据条数", description="默认 100, 最大 1000", examples=[100]
    )
    enable_cache: bool = Field(
        True, title="启用缓存", description="是否优先从本地缓存读取"
    )
    enable_test: bool = Field(
        False, title="启用测试模式(返回假数据)", description="仅用于调试"
    )


class MarketOrderRequest(BaseSymbolRequest):
    side: SideType = Field(..., title="方向", examples=["buy", "sell"])
    amount: float = Field(..., title="数量", examples=[0.001])
    clientOrderId: str | None = Field(
        None, title="客户端自定义ID", examples=["my_order_1"]
    )
    model_config = {"extra": "allow"}


class LimitOrderRequest(BaseSymbolRequest):
    side: SideType = Field(..., title="方向", examples=["buy", "sell"])
    amount: float = Field(..., title="数量", examples=[0.001])
    price: float = Field(..., title="价格", examples=[40000.0])
    clientOrderId: str | None = Field(
        None, title="客户端自定义ID", examples=["my_order_2"]
    )
    timeInForce: str | None = Field(
        None,
        title="有效方式",
        description="GTC, IOC, FOK",
        examples=["GTC", "IOC", "FOK"],
    )
    postOnly: bool = Field(False, title="只做maker", examples=[False, True])
    model_config = {"extra": "allow"}


class StopMarketOrderRequest(BaseSymbolRequest):
    side: SideType = Field(..., title="方向", examples=["sell", "buy"])
    amount: float = Field(..., title="数量", examples=[0.001])
    reduceOnly: bool = Field(True, title="只减仓", examples=[True, False])
    triggerPrice: float | None = Field(
        None, title="触发价格", description="止损触发价格", examples=[39000.0]
    )
    clientOrderId: str | None = Field(
        None, title="客户端自定义ID", examples=["stop_loss_1"]
    )
    timeInForce: str | None = Field(
        None,
        title="有效方式",
        description="GTC, IOC, FOK",
        examples=["GTC", "IOC", "FOK"],
    )
    model_config = {"extra": "allow"}


class TakeProfitMarketOrderRequest(BaseSymbolRequest):
    side: SideType = Field(..., title="方向", examples=["sell", "buy"])
    amount: float = Field(..., title="数量", examples=[0.001])
    reduceOnly: bool = Field(True, title="只减仓", examples=[True, False])
    triggerPrice: float | None = Field(
        None, title="触发价格", description="止盈触发价格", examples=[42000.0]
    )
    clientOrderId: str | None = Field(
        None, title="客户端自定义ID", examples=["take_profit_1"]
    )
    timeInForce: str | None = Field(
        None,
        title="有效方式",
        description="GTC, IOC, FOK",
        examples=["GTC", "IOC", "FOK"],
    )
    model_config = {"extra": "allow"}


class ClosePositionRequest(BaseSymbolRequest):
    side: PositionSide | None = Field(
        None,
        title="方向",
        description="指定平仓方向 (long/short), 不传则全平",
        examples=["long", "short"],
    )
    model_config = {"extra": "allow"}


class CancelAllOrdersRequest(BaseExchangeRequest):
    symbol: str | None = Field(None, title="交易对", examples=["BTC/USDT"])
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
            title="交易对列表",
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

    symbol: str | None = Field(None, title="交易对", examples=["BTC/USDT"])
    id: str = Field(..., title="订单ID", examples=["1234567890"])


# MarketInfoResponse has been moved to src/responses.py
