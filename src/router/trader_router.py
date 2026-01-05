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


# Pydantic 请求体模型
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


class OHLCVParams(BaseModel):
    exchange_name: str
    symbol: str
    period: str
    start_time: int | None = None
    count: int | None = None
    enable_cache: bool = True
    enable_test: bool = False
    file_type: str = ".parquet"
    cache_size: int = 1000
    page_size: int = 1500
    sandbox: bool = False


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
        ohlcv_data = fetch_ohlcv_ccxt(**params.model_dump(), cache_dir=OHLCV_DIR)
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
