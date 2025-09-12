from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional


# 从主应用中导入你的交易所实例和验证函数
from src.tools.shared import verify_token, config

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
from src.tools.adjust_trade_utils_decimal import (
    adjust_coin_amount_wrapper,
    adjust_usd_to_coin_amount_wrapper,
    adjusted_market_price_wrapper,
)

# 创建 APIRouter 实例
ccxt_router = APIRouter(
    prefix="/ccxt",  # 设置所有路由的前缀，例如 /ccxt/balance
    dependencies=[Depends(verify_token)],  # 设置全局依赖项
)


# Pydantic 请求体模型
class MarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    is_usd_amount: bool = False


class LimitOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    price: float


class StopMarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    reduceOnly: bool = True
    stopLossPrice: float | None = None


class TakeProfitMarketOrderRequest(BaseModel):
    exchange_name: str
    symbol: str
    side: str
    amount: float
    reduceOnly: bool = True
    takeProfitPrice: float | None = None


class StopMarketOrderPercentageRequest(BaseModel):
    amount_percentage: float = 1
    exchange_name: str
    symbol: str
    side: str
    reduceOnly: bool = True
    stopLossPrice: float | None = None


class TakeProfitMarketOrderPercentageRequest(BaseModel):
    amount_percentage: float = 1
    exchange_name: str
    symbol: str
    side: str
    reduceOnly: bool = True
    takeProfitPrice: float | None = None


class CloseAllOrderRequest(BaseModel):
    exchange_name: str
    symbol: str


class CancelAllOrdersRequest(BaseModel):
    exchange_name: str
    symbol: str


@ccxt_router.get("/tickers")
def get_tickers(
    exchange_name: str,
    symbols: str | None = None,
):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    try:
        result = fetch_tickers_ccxt(exchange_name, symbols)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/ohlcv")
def get_ohlcv(
    exchange_name: str,
    symbol: str,
    period: str,
    start_time: int | None = None,
    count: int | None = None,
    enable_cache: bool = True,
    enable_test: bool = False,
    file_type: str = ".parquet",  # .parquet or .csv
    cache_size: int = 1000,
    page_size: int = 1500,
    cache_dir: str = "./database",
):
    """
    获取 OHLCV（开盘价、最高价、最低价、收盘价、成交量）数据。
    """
    try:
        ohlcv_data = fetch_ohlcv_ccxt(
            exchange_name=exchange_name,
            symbol=symbol,
            period=period,
            start_time=start_time,
            count=count,
            enable_cache=enable_cache,
            enable_test=enable_test,
            file_type=file_type,
            cache_size=cache_size,
            page_size=page_size,
            cache_dir=cache_dir,
        )
        return ohlcv_data
    except HTTPException as e:
        raise e
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/balance")
def get_balance(exchange_name: str):
    try:
        result = fetch_balance_ccxt(exchange_name)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/market")
def create_market_order(order: MarketOrderRequest):
    """
    在指定交易所创建市价订单。
    """
    try:
        result = create_order_ccxt(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            type="market",
            side=order.side,
            amount=order.amount,
            price=None,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/limit")
def create_limit_order(order: LimitOrderRequest):
    """
    在指定交易所创建限价订单。
    """
    try:
        result = create_order_ccxt(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            type="limit",
            side=order.side,
            amount=order.amount,
            price=order.price,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/stop_market")
def create_stop_market_order(order: StopMarketOrderRequest):
    """
    在指定交易所创建止损市价订单。
    """
    try:
        type = "market"
        if order.exchange_name == "binance":
            type = "STOP_MARKET"

        result = create_order_ccxt(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            type=type,
            side=order.side,
            amount=order.amount,
            price=None,
            params={
                "reduceOnly": order.reduceOnly,
                "stopLossPrice": order.stopLossPrice,
            },
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/take_profit_market")
def create_take_profit_market_order(order: TakeProfitMarketOrderRequest):
    """
    在指定交易所创建止盈市价订单。
    """
    try:
        type = "market"
        if order.exchange_name == "binance":
            type = "TAKE_PROFIT_MARKET"

        result = create_order_ccxt(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            type=type,
            side=order.side,
            amount=order.amount,
            price=None,
            params={
                "reduceOnly": order.reduceOnly,
                "takeProfitPrice": order.takeProfitPrice,
            },
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/stop_market_percentage")
def create_stop_market_order_percentage(order: StopMarketOrderPercentageRequest):
    """
    在指定交易所创建基于百分比的止损市价订单。
    根据仓位大小的百分比来计算订单数量。
    """
    try:
        type = "market"
        if order.exchange_name == "binance":
            type = "STOP_MARKET"

        extra_params = {
            "reduceOnly": order.reduceOnly,
            "stopLossPrice": order.stopLossPrice,
        }
        result = create_exit_percentage_order(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            side=order.side,
            type=type,
            amount_percentage=order.amount_percentage,
            params=extra_params,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating percentage stop market order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/take_profit_market_percentage")
def create_take_profit_market_order_percentage(
    order: TakeProfitMarketOrderPercentageRequest,
):
    """
    在指定交易所创建基于百分比的止盈市价订单。
    根据仓位大小的百分比来计算订单数量。
    """
    try:
        type = "market"
        if order.exchange_name == "binance":
            type = "TAKE_PROFIT_MARKET"

        extra_params = {
            "reduceOnly": order.reduceOnly,
            "takeProfitPrice": order.takeProfitPrice,
        }
        result = create_exit_percentage_order(
            exchange_name=order.exchange_name,
            symbol=order.symbol,
            side=order.side,
            type=type,
            amount_percentage=order.amount_percentage,
            params=extra_params,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating percentage take profit market order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/close_all_orders")
def close_all_orders(order: CloseAllOrderRequest):
    """
    关闭指定品种所有仓位和挂单
    """
    try:
        result = close_all_order_ccxt(order.exchange_name, order.symbol)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error creating order: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/cancel_all_orders")
def cancel_all_orders(order: CancelAllOrdersRequest):
    """
    取消指定交易对所有挂单
    """
    try:
        result = cancel_all_orders_ccxt(
            order.exchange_name,
            order.symbol,
        )
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error cancelling all orders: {e}")
        raise HTTPException(status_code=500, detail=str(e))
