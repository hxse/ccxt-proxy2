# src/tools/ccxt_utils.py
from typing import Optional, Literal
from fastapi import HTTPException, status
import ccxt

# 从主应用中导入你的交易所实例和验证函数
from src.tools.shared import (
    binance_exchange_sandbox,
    binance_exchange_live,
    kraken_exchange_sandbox,
    kraken_exchange_live,
    config,
)
from src.tools.adjust_trade_utils_decimal import get_symbol
from src.cache_tool.cache_entry import (
    get_ohlcv_with_cache_lock,
    fetch_ohlcv,
    mock_fetch_ohlcv,
)
from src.cache_tool.cache_utils import sanitize_symbol

from src.tools.adjust_trade_utils_decimal import (
    adjust_coin_amount_wrapper,
    adjust_usd_to_coin_amount_wrapper,
    adjusted_market_price_wrapper,
)


def get_currency_type(is_usd_amount: bool):
    return "usd" if is_usd_amount else "coin"


def get_exchange_instance(exchange_name: str, sandbox: bool = True):
    """根据交易所名称获取 CCXT 交易所实例。"""
    if exchange_name == "binance":
        return binance_exchange_sandbox if sandbox else binance_exchange_live
    elif exchange_name == "kraken":
        return kraken_exchange_sandbox if sandbox else kraken_exchange_live
    else:
        raise HTTPException(status_code=400, detail="Invalid exchange name")


def fetch_tickers_ccxt(
    exchange_name: str,
    symbols: str | None = None,
    params: dict = {},
    sandbox: bool = False,
):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbols_list = None
    if symbols:
        symbols_list = [s.strip() for s in symbols.split(",")]
        symbols_list = [get_symbol(exchange_name, s) for s in symbols_list]
    tickers = exchange.fetch_tickers(symbols_list, params=params)
    return {"tickers": tickers}


def fetch_ohlcv_ccxt(
    exchange_name: str,
    symbol: str,
    period: str,
    start_time: Optional[int] = None,
    count: Optional[int] = None,
    enable_cache: bool = True,
    enable_test: bool = False,
    file_type: str = ".parquet",
    cache_size: int = 1000,
    page_size: int = 1500,
    cache_dir: str = "./data",
    sandbox: bool = False,
):
    """
    获取 OHLCV（开盘价、最高价、最低价、收盘价、成交量）数据。
    """
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbol_to_use = get_symbol(exchange_name, symbol)

    print("market_type:", config["market_type"])
    ohlcv_df = get_ohlcv_with_cache_lock(
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


def fetch_balance_ccxt(exchange_name: str, params: dict = {}, sandbox: bool = True):
    """
    获取指定交易所的余额信息。
    """
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    balance = exchange.fetch_balance(params=params)
    return {"balance": balance}


def create_order_ccxt(
    exchange_name: str,
    symbol: str,
    type: str,
    side: str,
    amount: float,
    price: float | None = None,
    is_usd_amount: bool = False,
    params: dict = {},
    sandbox: bool = True,
):
    """
    在指定交易所创建订单。
    """

    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbol_to_use = get_symbol(exchange_name, symbol)

    if is_usd_amount:
        adjusted_amount = adjust_usd_to_coin_amount_wrapper(
            exchange, symbol_to_use, amount
        )
        print(f"convert amount {amount} usd -> {adjusted_amount} coin")
    else:
        adjusted_amount = adjust_coin_amount_wrapper(exchange, symbol_to_use, amount)
        print(f"convert amount {amount} coin -> {adjusted_amount} coin")

    adjusted_price = adjusted_market_price_wrapper(exchange, symbol_to_use, price)
    print(f"convert price {price} -> {adjusted_price}")

    result = exchange.create_order(
        symbol_to_use, type, side, adjusted_amount, adjusted_price, params=params
    )
    return {"order": result}


def close_all_order_ccxt(
    exchange_name: str,
    symbol: str,
    params: dict = {},
    sandbox: bool = True,
):
    """
    关闭指定品种所有仓位和挂单
    """
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbol_to_use = get_symbol(exchange_name, symbol)

    params = {"reduceOnly": True}
    positions = exchange.fetch_positions([symbol_to_use])
    for i in positions:
        side = "sell" if i["side"] == "long" else "buy"
        amount = i["contracts"]
        exchange.create_order(symbol_to_use, "market", side, amount, params=params)
    remaining_positions = exchange.fetch_positions([symbol_to_use])
    return {"remaining_positions": remaining_positions}


def cancel_all_orders_ccxt(
    exchange_name: str,
    symbol: str,
    params: dict = {},
    sandbox: bool = True,
):
    """
    取消指定交易对的所有挂单。
    """
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbol_to_use = get_symbol(exchange_name, symbol)
    result = exchange.cancelAllOrders(symbol_to_use, params)
    return {"result": result}


def create_exit_percentage_order(
    exchange_name: str,
    symbol: str,
    side: str,
    type: str,
    amount_percentage: float,
    params: dict = {},
    is_usd_amount: bool = False,
    sandbox: bool = True,
):
    results = []  # 初始化 results 列表
    exchange = get_exchange_instance(exchange_name, sandbox=sandbox)
    symbol_to_use = get_symbol(exchange_name, symbol)

    positions = exchange.fetch_positions([symbol_to_use])

    for i in positions:
        if i["symbol"] == symbol_to_use:
            # 检查仓位方向与订单方向是否匹配，以进行平仓
            # 如果订单是 'sell' 且仓位是 'long'，或者订单是 'buy' 且仓位是 'short'
            if (side == "sell" and i["side"] == "long") or (
                side == "buy" and i["side"] == "short"
            ):
                # 不完全确定contracts的参数类型,所以这里的is_usd_amount最好保持false,避免未知情况
                target_amount = i["contracts"] * amount_percentage

                if target_amount > 0:  # 只有当计算出的订单数量大于0时才创建订单
                    order_result = create_order_ccxt(
                        exchange_name=exchange_name,
                        symbol=symbol,
                        type=type,
                        side=side,
                        amount=target_amount,
                        price=None,
                        is_usd_amount=is_usd_amount,
                        params=params,
                        sandbox=sandbox,
                    )
                    results.append(order_result)
                break

    if not results:
        return {
            "status": "error",
            "detail": "没有找到匹配的仓位或计算出的订单数量为零。",
        }
    return results
