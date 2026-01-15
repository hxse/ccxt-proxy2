from pydantic import BaseModel
from typing import Dict, List, Any, Optional
from src.responses import OrderStructure, PositionStructure
from src.base_types import SideType


class TradeStructure(BaseModel):
    """CCXT 统一成交结构"""

    id: str
    timestamp: Optional[int] = None
    datetime: Optional[str] = None
    symbol: str
    order: Optional[str] = None
    type: Optional[str] = None
    side: SideType
    takerOrMaker: Optional[str] = None
    price: float
    amount: float
    cost: float
    fee: Optional[Dict[str, Any]] = None
    info: Optional[Dict[str, Any]] = None

    model_config = {"extra": "allow"}


class OrdersResponse(BaseModel):
    """订单列表响应 (用于 open/closed orders)"""

    orders: List[OrderStructure]


class TradesResponse(BaseModel):
    """成交列表响应"""

    trades: List[TradeStructure]


class PositionsResponse(BaseModel):
    """持仓列表响应"""

    positions: List[PositionStructure]


class GenericResponse(BaseModel):
    """通用响应 (用于 setLeverage, setMarginMode)"""

    result: Dict[str, Any]
