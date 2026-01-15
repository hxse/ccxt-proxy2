from fastapi import APIRouter, Depends, HTTPException
from src.tools.ccxt_utils import (
    fetch_tickers_ccxt,
    fetch_ohlcv_ccxt,
    fetch_balance_ccxt,
    fetch_market_info_ccxt,
    create_market_order_ccxt,
    create_limit_order_ccxt,
    create_stop_market_order_ccxt,
    create_take_profit_market_order_ccxt,
    close_position_ccxt,
    cancel_all_orders_ccxt,
    fetch_order_ccxt,
)
from src.router.auth_handler import manager
from src.types import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
    ClosePositionRequest,
    CancelAllOrdersRequest,
    OHLCVParams,
    BalanceRequest,
    TickersRequest,
    MarketInfoRequest,
    FetchOrderRequest,
)
from src.responses import (
    TickersResponse,
    BalanceResponse,
    OrderResponse,
    MarketInfoResponse,
    ClosePositionResponse,
    CancelAllOrdersResponse,
)

# 创建文件处理路由，并添加鉴权依赖
ccxt_router = APIRouter(
    prefix="/ccxt", dependencies=[Depends(manager)], tags=["CCXT PROXY"]
)


@ccxt_router.get("/fetch_balance", response_model=BalanceResponse)
def get_balance(params: BalanceRequest = Depends()):
    try:
        result = fetch_balance_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/fetch_tickers", response_model=TickersResponse)
def get_tickers(params: TickersRequest = Depends()):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    try:
        result = fetch_tickers_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/fetch_ohlcv", response_model=list[list[float]])
def get_ohlcv(params: OHLCVParams = Depends()):
    """
    获取 OHLCV（开盘价、最高价、最低价、收盘价、成交量）数据。
    """
    try:
        ohlcv_data = fetch_ohlcv_ccxt(params)
        return ohlcv_data
    except HTTPException as e:
        print(e)
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/fetch_market_info", response_model=MarketInfoResponse)
def get_market_info(params: MarketInfoRequest = Depends()):
    """
    获取市场元数据 (用于下单计算)

    返回精度、最小数量、合约类型、杠杆等信息。
    """
    try:
        result = fetch_market_info_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/fetch_order", response_model=OrderResponse)
def get_order(params: FetchOrderRequest = Depends()):
    """
    获取特定订单详情
    注意kraken目前不支持
    """
    try:
        result = fetch_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/create_market_order", response_model=OrderResponse)
def create_market_order(params: MarketOrderRequest):
    """
    在指定交易所创建市价订单。
    """
    try:
        result = create_market_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/create_limit_order", response_model=OrderResponse)
def create_limit_order(params: LimitOrderRequest):
    """
    在指定交易所创建限价订单。
    """
    try:
        result = create_limit_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/create_stop_market_order", response_model=OrderResponse)
def create_stop_market_order(params: StopMarketOrderRequest):
    """
    在指定交易所创建止损市价订单。
    """
    try:
        result = create_stop_market_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/create_take_profit_market_order", response_model=OrderResponse)
def create_take_profit_market_order(params: TakeProfitMarketOrderRequest):
    """
    在指定交易所创建止盈市价订单。
    """
    try:
        result = create_take_profit_market_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/close_position", response_model=ClosePositionResponse)
def close_position(params: ClosePositionRequest):
    """
    关闭指定品种的当前仓位 (不包含限价挂单和止盈止损挂单)。
    "side": "long" "short" null, 如果是null就平仓所有方向


    Equivalent to Close Position.
    """
    try:
        result = close_position_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/cancel_all_orders", response_model=CancelAllOrdersResponse)
def cancel_all_orders(params: CancelAllOrdersRequest):
    """
    取消指定交易对所有挂单
    """
    try:
        result = cancel_all_orders_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error cancelling all orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("hello world2")
