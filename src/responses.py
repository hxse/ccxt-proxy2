from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from src.base_types import PositionSide

# === Market Info ===


class MarketInfoResponse(BaseModel):
    """市场信息响应 - 用于下单计算"""

    symbol: str
    linear: bool  # 是否U本位 (True=U本位, False=币本位)
    settle: str  # 结算货币 (USDT/BTC/USD)
    precision_amount: float  # 数量精度
    min_amount: float  # 最小下单数量
    contract_size: float  # 合约乘数
    leverage: int  # 当前杠杆


# === Tickers ===


class TickerInfo(BaseModel):
    """单个 Ticker 信息 (通用字段)"""

    symbol: str
    timestamp: Optional[int] = None
    datetime: Optional[str] = None
    high: Optional[float] = None
    low: Optional[float] = None
    bid: Optional[float] = None
    bidVolume: Optional[float] = None
    ask: Optional[float] = None
    askVolume: Optional[float] = None
    vwap: Optional[float] = None
    open: Optional[float] = None
    close: Optional[float] = None
    last: Optional[float] = None
    previousClose: Optional[float] = None
    change: Optional[float] = None
    percentage: Optional[float] = None
    average: Optional[float] = None
    baseVolume: Optional[float] = None
    quoteVolume: Optional[float] = None
    info: Optional[Dict[str, Any]] = None

    # 允许交易所特有字段 (如 Binance 的 markPrice, indexPrice)
    model_config = {"extra": "allow"}


class TickersResponse(BaseModel):
    """行情响应"""

    tickers: Dict[str, TickerInfo]


# === Balance ===


class BalanceItem(BaseModel):
    """单个币种余额 (e.g. balance['USDT'])"""

    free: float
    used: float
    total: float


class BalanceStructure(BaseModel):
    """余额数据结构"""

    free: Dict[str, float]
    used: Dict[str, float]
    total: Dict[str, float]
    timestamp: Optional[int] = None
    datetime: Optional[str] = None
    info: Optional[Dict[str, Any]] = None

    # 余额响应中还包含了具体币种的 key (如 'USDT': {...})
    model_config = {"extra": "allow"}


class BalanceResponse(BaseModel):
    """余额响应 (包含 free/used/total 汇总和具体币种详情)"""

    balance: BalanceStructure


# === Order ===


class OrderStructure(BaseModel):
    """
    通用订单结构
    基于 Binance 和 Kraken 对比得出的 27 个通用字段。
    """

    # --- CCXT Unified Core Fields (18 fields) ---
    id: str
    clientOrderId: Optional[str] = None
    datetime: Optional[str] = None
    timestamp: Optional[int] = None
    lastTradeTimestamp: Optional[int] = None
    status: str  # open, closed, canceled, ...
    symbol: str
    type: str  # limit, market, ...
    side: str  # buy, sell
    price: Optional[float] = None
    amount: Optional[float] = None
    filled: Optional[float] = None
    remaining: Optional[float] = None
    cost: Optional[float] = None
    average: Optional[float] = None
    trades: Optional[List[Any]] = None
    fee: Optional[Dict[str, Any]] = None
    info: Optional[Dict[str, Any]] = None

    # --- CCXT Unified Parameters/Extension Fields ---
    triggerPrice: Optional[float] = None
    reduceOnly: Optional[bool] = None
    postOnly: Optional[bool] = None
    timeInForce: Optional[str] = None

    # 允许交易所特有字段
    model_config = {"extra": "allow"}


class OrderResponse(BaseModel):
    """订单响应"""

    order: OrderStructure


# === OHLCV ===

# === OHLCV ===

OHLCVItem = List[float]  # [time, open, high, low, close, volume]


class OHLCVResponse(BaseModel):
    # Route response_model use List[OHLCVItem]
    pass


class CancelAllOrdersResponse(BaseModel):
    result: List[OrderStructure] | Any


class PositionStructure(BaseModel):
    """CCXT 持仓结构"""

    symbol: str
    timestamp: Optional[int] = None
    datetime: Optional[str] = None
    contracts: Optional[float] = None
    contractSize: Optional[float] = None
    side: Optional[PositionSide] = None  # long, short

    notional: Optional[float] = None
    leverage: Optional[float] = None
    collateral: Optional[float] = None
    entryPrice: Optional[float] = None
    markPrice: Optional[float] = None
    liquidationPrice: Optional[float] = None
    hedged: Optional[bool] = None

    # CCXT Standard Fields
    unrealizedPnl: Optional[float] = None
    percentage: Optional[float] = None
    maintenanceMargin: Optional[float] = None
    initialMargin: Optional[float] = None
    marginRatio: Optional[float] = None
    marginMode: Optional[str] = None  # 'cross' or 'isolated'

    info: Optional[Dict[str, Any]] = None
    model_config = {"extra": "allow"}


class ClosePositionResponse(BaseModel):
    remaining_positions: List[PositionStructure]
