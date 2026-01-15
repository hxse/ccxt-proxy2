from fastapi import APIRouter, Depends, HTTPException
from src.router.auth_handler import manager
from src.types_extended import (
    FetchOpenOrdersRequest,
    FetchClosedOrdersRequest,
    FetchMyTradesRequest,
    FetchPositionsRequest,
    SetLeverageRequest,
    SetMarginModeRequest,
    CancelOrderRequest,
)
from src.responses_extended import (
    OrdersResponse,
    TradesResponse,
    PositionsResponse,
    GenericResponse,
)
from src.responses import OrderResponse  # Reuse existing OrderResponse
from src.tools.ccxt_utils_extended import (
    fetch_open_orders_ccxt,
    fetch_closed_orders_ccxt,
    fetch_my_trades_ccxt,
    fetch_positions_ccxt,
    set_leverage_ccxt,
    set_margin_mode_ccxt,
    cancel_order_ccxt,
)

extended_router = APIRouter(
    prefix="/ccxt", dependencies=[Depends(manager)], tags=["CCXT PROXY EXTENDED"]
)


@extended_router.get("/fetch_open_orders", response_model=OrdersResponse)
def get_open_orders(params: FetchOpenOrdersRequest = Depends()):
    """获取当前挂单
    包括限价挂单和止盈止损挂单, 不包括持仓
    """
    try:
        return fetch_open_orders_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/fetch_closed_orders", response_model=OrdersResponse)
def get_closed_orders(params: FetchClosedOrdersRequest = Depends()):
    """获取历史订单"""
    try:
        return fetch_closed_orders_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/fetch_my_trades", response_model=TradesResponse)
def get_my_trades(params: FetchMyTradesRequest = Depends()):
    """获取成交记录"""
    try:
        return fetch_my_trades_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/fetch_positions", response_model=PositionsResponse)
def get_positions(params: FetchPositionsRequest = Depends()):
    """
    获取持仓信息
    不包括限价挂单和止盈止损挂单
    """
    try:
        return fetch_positions_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.post("/set_leverage", response_model=GenericResponse)
def set_leverage(params: SetLeverageRequest):
    """设置杠杆"""
    try:
        return set_leverage_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.post("/set_margin_mode", response_model=GenericResponse)
def set_margin_mode(params: SetMarginModeRequest):
    """设置保证金模式 (cross/isolated)
    kraken不支持设置保证金模式
    """
    try:
        return set_margin_mode_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.post("/cancel_order", response_model=OrderResponse)
def cancel_order(params: CancelOrderRequest):
    """取消单个订单
    包括限价挂单和止盈止损挂单, 不包括持仓
    """
    try:
        return cancel_order_ccxt(params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
