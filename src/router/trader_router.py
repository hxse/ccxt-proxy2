# routers/trade.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

# 从主应用中导入你的交易所实例和验证函数
from src.tools.shared import (
    verify_token,
    binance_exchange,
    kraken_exchange,
    kraken_symbol_dict,
    config,
)
from src.cache_tool.handle_cache import (
    get_ohlcv_with_cache,
    fetch_ohlcv,
    mock_fetch_ohlcv,
    sanitize_symbol,
)
from datetime import datetime, timezone
import numpy as np


# 创建 APIRouter 实例
ccxt_router = APIRouter(
    prefix="/ccxt",  # 设置所有路由的前缀，例如 /ccxt/balance
    dependencies=[Depends(verify_token)],  # 设置全局依赖项
)


class Order(BaseModel):
    symbol: str
    side: str
    amount: float
    exchange_name: str


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
        if exchange_name == "binance":
            exchange = binance_exchange
            symbol_to_use = symbol
        elif exchange_name == "kraken":
            exchange = kraken_exchange
            symbol_to_use = kraken_symbol_dict.get(symbol, symbol)
        else:
            raise HTTPException(status_code=400, detail="Invalid exchange name")

        print("market_type:", config["market_type"])
        ohlcv_df = get_ohlcv_with_cache(
            symbol=symbol_to_use,
            period=period,
            start_time=start_time,
            count=count,
            cache_dir=f"{cache_dir}/{exchange_name}/{config['market_type']}/{sanitize_symbol(symbol)}/{period}",
            cache_size=cache_size,
            page_size=page_size,
            enable_cache=enable_cache,
            file_type=file_type,
            fetch_callback=mock_fetch_ohlcv if enable_test else fetch_ohlcv,
            fetch_callback_params={"exchange": exchange},
        )

        return ohlcv_df.to_numpy().tolist()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.get("/balance")
def get_balance(exchange_name: str):
    try:
        if exchange_name == "binance":
            balance = binance_exchange.fetch_balance()
        elif exchange_name == "kraken":
            balance = kraken_exchange.fetch_balance()
        else:
            raise HTTPException(status_code=400, detail="Invalid exchange name")
        return {"balance": balance}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@ccxt_router.post("/order")
def create_order(order: Order):
    try:
        if order.exchange_name == "binance":
            exchange = binance_exchange
            symbol_to_use = order.symbol
        elif order.exchange_name == "kraken":
            exchange = kraken_exchange
            symbol_to_use = kraken_symbol_dict.get(order.symbol, order.symbol)
        else:
            raise HTTPException(status_code=400, detail="Invalid exchange name")

        result = exchange.create_order(
            symbol_to_use, "market", order.side, order.amount
        )
        return {"order": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
