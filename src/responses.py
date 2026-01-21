from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
from src.base_types import PositionSide

# === Market Info ===


class MarketInfoResponse(BaseModel):
    """
    市场信息响应 - 用于下单计算
    """

    symbol: str = Field(..., title="交易对", examples=["BTC/USDT"])
    linear: bool = Field(
        ...,
        title="是否U本位",
        description="True=U本位 (Linear), False=币本位 (Inverse)",
        examples=[True],
    )
    settle: str = Field(..., title="结算货币", examples=["USDT", "BTC"])
    precision_amount: float = Field(
        ...,
        title="数量精度 (步长)",
        description="下单数量的最小增量单位 (Step Size)。注意：这不是小数位数，而是步长值 (e.g. 0.001)。",
        examples=[0.001, 1.0],
        ge=0,
    )
    min_amount: float = Field(
        ...,
        title="最小下单数量",
        description="该交易对允许的单笔最小下单量。",
        examples=[0.001],
        ge=0,
    )
    contract_size: float = Field(
        ...,
        title="合约乘数",
        description="一张合约代表的币种数量 (通常为 1)",
        examples=[1.0],
    )
    leverage: int = Field(
        ...,
        title="当前杠杆",
        description="当前账户在该交易对上的杠杆倍数",
        examples=[10, 20],
    )


# === Tickers ===


class TickerInfo(BaseModel):
    """单个 Ticker 信息 (通用字段)"""

    symbol: str = Field(..., title="交易对", examples=["BTC/USDT"])
    timestamp: Optional[int] = Field(
        None,
        title="时间戳 (ms)",
        description="数据生成的时间戳",
        examples=[1672531200000],
    )
    datetime: Optional[str] = Field(
        None,
        title="时间字符串",
        description="ISO 8601 格式时间",
        examples=["2023-01-01T00:00:00Z"],
    )
    high: Optional[float] = Field(None, title="最高价", examples=[42000.5])
    low: Optional[float] = Field(None, title="最低价", examples=[41000.0])
    bid: Optional[float] = Field(None, title="买一价", examples=[41500.1])
    bidVolume: Optional[float] = Field(None, title="买一量", examples=[0.5])
    ask: Optional[float] = Field(None, title="卖一价", examples=[41500.2])
    askVolume: Optional[float] = Field(None, title="卖一量", examples=[1.2])
    vwap: Optional[float] = Field(None, title="成交量加权平均价", examples=[41550.0])
    open: Optional[float] = Field(None, title="开盘价", examples=[41200.0])
    close: Optional[float] = Field(None, title="收盘价", examples=[41600.0])
    last: Optional[float] = Field(None, title="最新成交价", examples=[41600.0])
    previousClose: Optional[float] = Field(None, title="前收盘价", examples=[41200.0])
    change: Optional[float] = Field(None, title="涨跌额", examples=[400.0])
    percentage: Optional[float] = Field(None, title="涨跌幅 (%)", examples=[0.97])
    average: Optional[float] = Field(None, title="平均价", examples=[41400.0])
    baseVolume: Optional[float] = Field(None, title="基础币种成交量", examples=[100.5])
    quoteVolume: Optional[float] = Field(
        None, title="报价币种成交量", examples=[4160000.0]
    )
    info: Optional[Dict[str, Any]] = Field(
        None, title="原始数据", description="交易所返回的原始完整数据"
    )

    # 允许交易所特有字段 (如 Binance 的 markPrice, indexPrice)
    model_config = {"extra": "allow"}


class TickersResponse(BaseModel):
    """行情响应"""

    tickers: Dict[str, TickerInfo] = Field(
        ...,
        title="Tickers 字典",
        description="Key 为交易对名称，Value 为 Ticker 详情",
        examples=[
            {"BTC/USDT": {"symbol": "BTC/USDT", "last": 41600.0, "percentage": 0.97}}
        ],
    )


# === Balance ===


class BalanceItem(BaseModel):
    """单个币种余额 (e.g. balance['USDT'])"""

    free: float = Field(..., title="可用余额", examples=[100.5])
    used: float = Field(..., title="冻结/占用余额", examples=[10.0])
    total: float = Field(..., title="总余额", examples=[110.5])


class BalanceStructure(BaseModel):
    """余额数据结构"""

    free: Dict[str, float] = Field(
        ..., title="可用余额字典", examples=[{"USDT": 100.5}]
    )
    used: Dict[str, float] = Field(..., title="占用余额字典", examples=[{"USDT": 10.0}])
    total: Dict[str, float] = Field(..., title="总余额字典", examples=[{"USDT": 110.5}])
    timestamp: Optional[int] = Field(None, title="时间戳", examples=[1672531200000])
    datetime: Optional[str] = Field(
        None, title="时间字符串", examples=["2023-01-01T00:00:00Z"]
    )
    info: Optional[Dict[str, Any]] = Field(
        None, title="原始数据", description="交易所返回的原始完整数据"
    )

    # 余额响应中还包含了具体币种的 key (如 'USDT': {...})
    model_config = {"extra": "allow"}


class BalanceResponse(BaseModel):
    """余额响应 (包含 free/used/total 汇总和具体币种详情)"""

    balance: BalanceStructure = Field(..., title="余额详情")


# === Order ===


class OrderStructure(BaseModel):
    """
    通用订单结构
    基于 Binance 和 Kraken 对比得出的 27 个通用字段。
    """

    # --- CCXT Unified Core Fields (18 fields) ---
    id: str = Field(..., title="订单ID", examples=["1234567890"])
    clientOrderId: Optional[str] = Field(
        None, title="客户端自定义ID", examples=["my_order_1"]
    )
    datetime: Optional[str] = Field(
        None, title="时间字符串", examples=["2023-01-01T00:00:00Z"]
    )
    timestamp: Optional[int] = Field(
        None, title="时间戳 (ms)", description="订单创建时间", examples=[1672531200000]
    )
    lastTradeTimestamp: Optional[int] = Field(
        None,
        title="最后交易时间戳",
        description="最近一次成交的时间",
        examples=[1672531200000],
    )
    status: str = Field(
        ...,
        title="订单状态",
        description="open, closed, canceled, expired, rejected",
        examples=["open", "closed", "canceled", "expired", "rejected"],
    )
    symbol: str = Field(..., title="交易对", examples=["BTC/USDT"])
    type: str = Field(
        ...,
        title="订单类型",
        description="limit, market, ...",
        examples=["limit", "market", "STOP_MARKET", "TAKE_PROFIT_MARKET"],
    )
    side: str = Field(
        ..., title="方向", description="buy, sell", examples=["buy", "sell"]
    )
    price: Optional[float] = Field(None, title="委托价格", examples=[42000.0])
    amount: Optional[float] = Field(None, title="委托数量", examples=[1.0])
    filled: Optional[float] = Field(None, title="已成交数量", examples=[0.5])
    remaining: Optional[float] = Field(None, title="剩余数量", examples=[0.5])
    cost: Optional[float] = Field(
        None, title="成交金额", description="filled * average price", examples=[21000.0]
    )
    average: Optional[float] = Field(None, title="成交均价", examples=[42000.0])
    trades: Optional[List[Any]] = Field(
        None, title="成交明细", description="该订单的成交记录列表"
    )
    fee: Optional[Dict[str, Any]] = Field(None, title="手续费信息")
    info: Optional[Dict[str, Any]] = Field(None, title="原始数据")

    # --- CCXT Unified Parameters/Extension Fields ---
    triggerPrice: Optional[float] = Field(
        None, title="触发价格", description="止盈止损触发价", examples=[40000.0]
    )
    reduceOnly: Optional[bool] = Field(
        None, title="只减仓", description="是否为只减仓订单", examples=[False, True]
    )
    postOnly: Optional[bool] = Field(
        None,
        title="只做maker",
        description="是否为 Post-Only 订单",
        examples=[False, True],
    )
    timeInForce: Optional[str] = Field(
        None,
        title="有效方式",
        description="GTC, IOC, FOK",
        examples=["GTC", "IOC", "FOK"],
    )

    # 允许交易所特有字段
    model_config = {"extra": "allow"}


class OrderResponse(BaseModel):
    """订单响应"""

    order: OrderStructure


# === OHLCV ===

OHLCVItem = List[float]  # [time, open, high, low, close, volume]


class OHLCVResponse(BaseModel):
    # Route response_model use List[OHLCVItem]
    pass


class CancelAllOrdersResponse(BaseModel):
    result: List[OrderStructure] | Any = Field(
        ..., title="取消结果", description="被取消的订单列表或原始响应"
    )


class PositionStructure(BaseModel):
    """CCXT 持仓结构"""

    symbol: str = Field(..., title="交易对", examples=["BTC/USDT"])
    timestamp: Optional[int] = Field(None, title="时间戳", examples=[1672531200000])
    datetime: Optional[str] = Field(
        None, title="时间字符串", examples=["2023-01-01T00:00:00Z"]
    )
    contracts: Optional[float] = Field(
        None, title="合约数量", description="持仓张数或币数", examples=[1.5]
    )
    contractSize: Optional[float] = Field(
        None, title="合约乘数", description="单张合约大小", examples=[1.0]
    )
    side: Optional[PositionSide] = Field(
        None, title="持仓方向", description="long, short", examples=["long"]
    )

    notional: Optional[float] = Field(
        None, title="名义价值", description="持仓总价值 (USDT)", examples=[63000.0]
    )
    leverage: Optional[float] = Field(
        None, title="杠杆倍数", description="当前持仓杠杆", examples=[10.0]
    )
    collateral: Optional[float] = Field(
        None, title="保证金", description="持仓保证金", examples=[6300.0]
    )
    entryPrice: Optional[float] = Field(None, title="开仓均价", examples=[42000.0])
    markPrice: Optional[float] = Field(None, title="标记价格", examples=[42100.0])
    liquidationPrice: Optional[float] = Field(
        None, title="强平价格", examples=[38000.0]
    )
    hedged: Optional[bool] = Field(None, title="是否对冲模式", examples=[False])

    # CCXT Standard Fields
    unrealizedPnl: Optional[float] = Field(None, title="未实现盈亏", examples=[150.0])
    percentage: Optional[float] = Field(None, title="盈亏比例 (%)", examples=[2.38])
    maintenanceMargin: Optional[float] = Field(
        None, title="维持保证金", examples=[300.0]
    )
    initialMargin: Optional[float] = Field(None, title="初始保证金", examples=[6300.0])
    marginRatio: Optional[float] = Field(None, title="保证金率", examples=[0.05])
    marginMode: Optional[str] = Field(
        None,
        title="保证金模式",
        description="cross (全仓), isolated (逐仓)",
        examples=["cross"],
    )

    info: Optional[Dict[str, Any]] = Field(None, title="原始数据")
    model_config = {"extra": "allow"}


class ClosePositionResponse(BaseModel):
    remaining_positions: List[PositionStructure] = Field(
        ...,
        title="剩余持仓列表",
        description="平仓操作后剩余的持仓（理论上应为空或变少）",
    )
