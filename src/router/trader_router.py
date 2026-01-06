from fastapi import APIRouter, Depends, HTTPException

# 导入封装的 ccxt 工具函数
from src.tools.ccxt_utils import (
    fetch_tickers_ccxt,
    fetch_ohlcv_ccxt,
    fetch_balance_ccxt,
    create_market_order_ccxt,
    create_limit_order_ccxt,
    create_stop_market_order_ccxt,
    create_take_profit_market_order_ccxt,
    create_stop_market_percentage_order_ccxt,
    create_take_profit_percentage_order_ccxt,
    close_all_order_ccxt,
    cancel_all_orders_ccxt,
)


from src.router.auth_handler import manager


# 创建文件处理路由，并添加鉴权依赖
ccxt_router = APIRouter(
    prefix="/ccxt", dependencies=[Depends(manager)], tags=["CCXT PROXY"]
)


from src.types import (
    MarketOrderRequest,
    LimitOrderRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
    StopMarketOrderPercentageRequest,
    TakeProfitMarketOrderPercentageRequest,
    CloseAllOrderRequest,
    CancelAllOrdersRequest,
    OHLCVParams,
    BalanceRequest,
    TickersRequest,
)


@ccxt_router.get("/balance")
def get_balance(params: BalanceRequest = Depends()):
    try:
        result = fetch_balance_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/tickers")
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


@ccxt_router.get("/ohlcv")
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


@ccxt_router.post("/market")
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


@ccxt_router.post("/limit")
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


@ccxt_router.post("/stop_market")
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


@ccxt_router.post("/take_profit_market")
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


@ccxt_router.post("/stop_market_percentage")
def create_stop_market_order_percentage(params: StopMarketOrderPercentageRequest):
    """
    在指定交易所创建基于百分比的止损市价订单。
    根据仓位大小的百分比来计算订单数量。
    """
    try:
        result = create_stop_market_percentage_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating percentage stop market order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/take_profit_market_percentage")
def create_take_profit_market_order_percentage(
    params: TakeProfitMarketOrderPercentageRequest,
):
    """
    在指定交易所创建基于百分比的止盈市价订单。
    根据仓位大小的百分比来计算订单数量。
    """
    try:
        result = create_take_profit_percentage_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating percentage take profit market order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/close_all_orders")
def close_all_orders(params: CloseAllOrderRequest):
    """
    关闭指定品种所有仓位和挂单
    """
    try:
        result = close_all_order_ccxt(params)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/cancel_all_orders")
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
