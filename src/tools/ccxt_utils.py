from typing import Literal
from src.types import ExchangeName, MarketType, OrderType, SideType, ModeType


import polars as pl

from src.types import (
    BalanceRequest,
    TickersRequest,
    OHLCVParams,
    MarketOrderRequest,
    LimitOrderRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
    StopMarketOrderPercentageRequest,
    TakeProfitMarketOrderPercentageRequest,
    CloseAllOrderRequest,
    CancelAllOrdersRequest,
)

# 从主应用中导入你的交易所实例和验证函数
from src.tools.shared import (
    OHLCV_DIR,
)
from src.tools.exchange_manager import exchange_manager
from src.tools.adjust_trade_utils_decimal import get_symbol

# 新缓存系统引入
from src.cache_tool import get_ohlcv_with_cache, DataLocation


from src.tools.adjust_trade_utils_decimal import (
    adjust_coin_amount_wrapper,
    adjust_usd_to_coin_amount_wrapper,
    adjusted_market_price_wrapper,
)


def get_currency_type(is_usd_amount: bool):
    return "usd" if is_usd_amount else "coin"


def fetch_tickers_ccxt(request: TickersRequest):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbols_list = None
    if request.symbols:
        symbols_list = [s.strip() for s in request.symbols.split(",")]
        symbols_list = [get_symbol(request.exchange_name, s) for s in symbols_list]
    tickers = exchange.fetch_tickers(symbols_list)
    return {"tickers": tickers}


def fetch_ohlcv_ccxt(request: OHLCVParams):
    """
    获取 OHLCV（开盘价、最高价、最低价、收盘价、成交量）数据。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = get_symbol(request.exchange_name, request.symbol)

    # 根据 sandbox 推导 mode（用于缓存目录路径）
    mode: Literal["live", "demo"] = "demo" if request.mode == "sandbox" else "live"

    # 构造数据位置对象
    # 显式 cast period 以满足类型检查
    # 注意：这里假设调用者传入的 period 是合法的，实际应该在 Pydantic 层做校验
    loc = DataLocation(
        exchange=request.exchange_name,
        mode=mode,
        market=request.market,
        symbol=request.symbol,  # 使用标准 symbol 命名目录
        period=request.period,
    )

    def fetch_callback(
        symbol: str, period: str, start_time: int | None, count: int, **kwargs
    ) -> pl.DataFrame:
        exchange_instance = kwargs.get("exchange")
        limit = count if count is not None else 1500

        if exchange_instance is None:
            return pl.DataFrame()

        # 使用 ccxt 获取数据
        # 注意：start_time 为 None 时，ccxt 会获取最新数据
        data = exchange_instance.fetch_ohlcv(
            symbol_to_use, period, since=start_time, limit=limit
        )  # type: ignore

        if not data:
            return pl.DataFrame()

        df = pl.DataFrame(
            data,
            schema=["time", "open", "high", "low", "close", "volume"],
            orient="row",
        )
        return df.with_columns(
            [
                pl.col("time").cast(pl.Int64),
                pl.col("open").cast(pl.Float64),
                pl.col("high").cast(pl.Float64),
                pl.col("low").cast(pl.Float64),
                pl.col("close").cast(pl.Float64),
                pl.col("volume").cast(pl.Float64),
            ]
        )

    def mock_fetch_callback(
        symbol: str, period: str, start_time: int | None, count: int, **kwargs
    ) -> pl.DataFrame:
        # 简单模拟返回空数据，实际测试应在 test 环境中控制
        return pl.DataFrame()

    ohlcv_df = get_ohlcv_with_cache(
        base_dir=OHLCV_DIR,
        loc=loc,
        start_time=request.start_time,
        count=request.count or 100,  # 默认获取100条？
        fetch_callback=mock_fetch_callback if request.enable_test else fetch_callback,
        fetch_callback_params={"exchange": exchange},
        enable_cache=request.enable_cache,
    )

    return ohlcv_df.to_numpy().tolist()


def fetch_balance_ccxt(request: BalanceRequest):
    """
    获取指定交易所的余额信息。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    balance = exchange.fetch_balance()
    return {"balance": balance}


def create_order_ccxt(
    exchange_name: ExchangeName,
    mode: ModeType,
    market: MarketType,
    symbol: str,
    type: OrderType,
    side: SideType,
    amount: float,
    price: float | None = None,
    is_usd_amount: bool = False,
    params: dict = {},
):
    """
    在指定交易所创建订单。
    """

    exchange = exchange_manager.get(exchange_name, market, mode)
    symbol_to_use = get_symbol(exchange_name, symbol)

    if is_usd_amount:
        adjusted_amount = adjust_usd_to_coin_amount_wrapper(
            exchange, symbol_to_use, amount
        )
        print(f"convert amount {amount} usd -> {adjusted_amount} coin")
    else:
        adjusted_amount = adjust_coin_amount_wrapper(exchange, symbol_to_use, amount)
        print(f"convert amount {amount} coin -> {adjusted_amount} coin")

    adjusted_price = None
    if price is not None:
        adjusted_price = adjusted_market_price_wrapper(exchange, symbol_to_use, price)
    print(f"convert price {price} -> {adjusted_price}")

    result = exchange.create_order(
        symbol_to_use, type, side, adjusted_amount, adjusted_price, params=params
    )
    return {"order": result}


def create_market_order_ccxt(request: MarketOrderRequest):
    """创建市价订单。"""
    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="market",
        side=request.side,
        amount=request.amount,
        price=None,
        is_usd_amount=request.is_usd_amount,
    )


def create_limit_order_ccxt(request: LimitOrderRequest):
    """创建限价订单。"""
    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="limit",
        side=request.side,
        amount=request.amount,
        price=request.price,
        is_usd_amount=request.is_usd_amount,
    )


def create_stop_market_order_ccxt(request: StopMarketOrderRequest):
    """创建止损市价订单。"""
    order_type = "STOP_MARKET" if request.exchange_name == "binance" else "market"
    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type=order_type,
        side=request.side,
        amount=request.amount,
        price=None,
        is_usd_amount=request.is_usd_amount,
        params={
            "reduceOnly": request.reduceOnly,
            "stopLossPrice": request.stopLossPrice,
        },
    )


def create_take_profit_market_order_ccxt(request: TakeProfitMarketOrderRequest):
    """创建止盈市价订单。"""
    order_type = (
        "TAKE_PROFIT_MARKET" if request.exchange_name == "binance" else "market"
    )
    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type=order_type,
        side=request.side,
        amount=request.amount,
        price=None,
        is_usd_amount=request.is_usd_amount,
        params={
            "reduceOnly": request.reduceOnly,
            "takeProfitPrice": request.takeProfitPrice,
        },
    )


def close_all_order_ccxt(request: CloseAllOrderRequest):
    """
    关闭指定品种所有仓位和挂单
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = get_symbol(request.exchange_name, request.symbol)

    params = {"reduceOnly": True}
    positions = exchange.fetch_positions([symbol_to_use])
    for i in positions:
        side = "sell" if i["side"] == "long" else "buy"
        amount = i["contracts"]
        exchange.create_order(symbol_to_use, "market", side, amount, params=params)
    remaining_positions = exchange.fetch_positions([symbol_to_use])
    return {"remaining_positions": remaining_positions}


def cancel_all_orders_ccxt(request: CancelAllOrdersRequest):
    """
    取消指定交易对的所有挂单。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = get_symbol(request.exchange_name, request.symbol)
    result = exchange.cancelAllOrders(symbol_to_use)
    return {"result": result}


def create_exit_percentage_order(
    exchange_name: ExchangeName,
    symbol: str,
    side: SideType,
    type: OrderType,
    amount_percentage: float,
    params: dict = {},
    is_usd_amount: bool = False,
    mode: ModeType = "sandbox",
    market: MarketType = "future",
):
    results = []  # 初始化 results 列表
    exchange = exchange_manager.get(exchange_name, market, mode)
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
                        mode=mode,
                        market=market,
                        symbol=symbol,
                        type=type,
                        side=side,
                        amount=target_amount,
                        price=None,
                        is_usd_amount=is_usd_amount,
                        params=params,
                    )
                    results.append(order_result)
                break

    if not results:
        return {
            "status": "error",
            "detail": "没有找到匹配的仓位或计算出的订单数量为零。",
        }
    return results


def create_stop_market_percentage_order_ccxt(request: StopMarketOrderPercentageRequest):
    """创建基于百分比的止损市价订单。"""
    order_type = "STOP_MARKET" if request.exchange_name == "binance" else "market"
    return create_exit_percentage_order(
        exchange_name=request.exchange_name,
        symbol=request.symbol,
        side=request.side,
        type=order_type,
        amount_percentage=request.amount_percentage,
        params={
            "reduceOnly": request.reduceOnly,
            "stopLossPrice": request.stopLossPrice,
        },
        is_usd_amount=request.is_usd_amount,
        mode=request.mode,
        market=request.market,
    )


def create_take_profit_percentage_order_ccxt(
    request: TakeProfitMarketOrderPercentageRequest,
):
    """创建基于百分比的止盈市价订单。"""
    order_type = (
        "TAKE_PROFIT_MARKET" if request.exchange_name == "binance" else "market"
    )
    return create_exit_percentage_order(
        exchange_name=request.exchange_name,
        symbol=request.symbol,
        side=request.side,
        type=order_type,
        amount_percentage=request.amount_percentage,
        params={
            "reduceOnly": request.reduceOnly,
            "takeProfitPrice": request.takeProfitPrice,
        },
        is_usd_amount=request.is_usd_amount,
        mode=request.mode,
        market=request.market,
    )
