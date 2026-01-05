from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# 从主应用中导入你的交易所实例和验证函数

# 导入封装的 ccxt 工具函数
from src.tools.ccxt_utils import (
    fetch_tickers_ccxt,
    fetch_ohlcv_ccxt,
    fetch_balance_ccxt,
    create_order_ccxt,
    close_all_order_ccxt,
    cancel_all_orders_ccxt,
    get_exchange_instance,
    create_exit_percentage_order,
)

from src.tools.shared import config, OHLCV_DIR
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
)


@ccxt_router.get("/balance")
def get_balance(exchange_name: str, sandbox: bool = True):
    try:
        result = fetch_balance_ccxt(exchange_name, sandbox=sandbox)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/tickers")
def get_tickers(
    exchange_name: str,
    symbols: str | None = None,
    sandbox: bool = False,
):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    try:
        result = fetch_tickers_ccxt(exchange_name, symbols, sandbox=sandbox)
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
        # fetch_ohlcv_ccxt expects cache_dir as str, but params.cache_dir is Path (if using the new model)
        # However, we are passing cache_dir=OHLCV_DIR which is a Path object from shared.py
        # We need to convert it to string.
        params_dict = params.model_dump()
        if "cache_dir" in params_dict:
            # cache_dir provided in params is likely the default or user provided,
            # but here we seem to override it with OHLCV_DIR from shared.
            # Actually the original code was: fetch_ohlcv_ccxt(**params.model_dump(), cache_dir=OHLCV_DIR)
            # So we should just cast OHLCV_DIR to str.
            pass

        ohlcv_data = fetch_ohlcv_ccxt(
            **params.model_dump(exclude={"cache_dir"}), cache_dir=str(OHLCV_DIR)
        )
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
        result = create_order_ccxt(
            **params.model_dump(),
            type="market",
            price=None,
        )
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
        result = create_order_ccxt(
            **params.model_dump(),
            type="limit",
        )
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
        type = "market"
        if params.exchange_name == "binance":
            type = "STOP_MARKET"

        result = create_order_ccxt(
            **params.model_dump(exclude={"reduceOnly", "stopLossPrice"}),
            type=type,
            price=None,
            params={
                "reduceOnly": params.reduceOnly,
                "stopLossPrice": params.stopLossPrice,
            },
        )
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
        type = "market"
        if params.exchange_name == "binance":
            type = "TAKE_PROFIT_MARKET"

        result = create_order_ccxt(
            **params.model_dump(exclude={"reduceOnly", "takeProfitPrice"}),
            type=type,
            price=None,
            params={
                "reduceOnly": params.reduceOnly,
                "takeProfitPrice": params.takeProfitPrice,
            },
        )
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
        type = "market"
        if params.exchange_name == "binance":
            type = "STOP_MARKET"

        result = create_exit_percentage_order(
            **params.model_dump(exclude={"reduceOnly", "stopLossPrice"}),
            type=type,
            params={
                "reduceOnly": params.reduceOnly,
                "stopLossPrice": params.stopLossPrice,
            },
        )
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
        type = "market"
        if params.exchange_name == "binance":
            type = "TAKE_PROFIT_MARKET"

        result = create_exit_percentage_order(
            **params.model_dump(exclude={"reduceOnly", "takeProfitPrice"}),
            type=type,
            params={
                "reduceOnly": params.reduceOnly,
                "takeProfitPrice": params.takeProfitPrice,
            },
        )
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
        result = close_all_order_ccxt(**params.model_dump())
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
        result = cancel_all_orders_ccxt(**params.model_dump())
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error cancelling all orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))
