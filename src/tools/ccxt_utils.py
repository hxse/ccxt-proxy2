from typing import Literal
from src.base_types import (
    ExchangeName,
    MarketType,
    OrderType,
    SideType,
    ModeType,
)
from src.types import (
    BalanceRequest,
    TickersRequest,
    OHLCVParams,
    MarketOrderRequest,
    LimitOrderRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
    ClosePositionRequest,
    CancelAllOrdersRequest,
    MarketInfoRequest,
    FetchOrderRequest,
)
from src.responses import MarketInfoResponse
import polars as pl
from src.tools.shared import OHLCV_DIR
from src.tools.exchange_manager import exchange_manager
from src.cache_tool import get_ohlcv_with_cache, DataLocation
from src.tools import binance_adapter


def fetch_tickers_ccxt(request: TickersRequest):
    """
    获取指定交易所的交易对报价（tickers）数据。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbols_list = request.symbols_list  # 使用 property 获取列表

    tickers = exchange.fetch_tickers(symbols_list, params={})
    return {"tickers": tickers}


def fetch_ohlcv_ccxt(request: OHLCVParams):
    """
    获取 OHLCV（开盘价、最高价、最低价、收盘价、成交量）数据。
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = request.symbol

    # 根据 sandbox 推导 mode（用于缓存目录路径）
    mode: Literal["live", "demo"] = "demo" if request.mode == "sandbox" else "live"

    # 构造数据位置对象
    loc = DataLocation(
        exchange=request.exchange_name,
        mode=mode,
        market=request.market,
        symbol=request.symbol,  # 使用标准 symbol 命名目录
        period=request.timeframe,
    )

    def fetch_callback(
        symbol: str, period: str, start_time: int | None, count: int, **kwargs
    ) -> pl.DataFrame:
        exchange_instance = kwargs.get("exchange")
        limit = count if count is not None else 1500

        if exchange_instance is None:
            return pl.DataFrame()

        # 使用 ccxt 获取数据
        data = exchange_instance.fetch_ohlcv(
            symbol_to_use, period, since=start_time, limit=limit
        )

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
        return pl.DataFrame()

    ohlcv_df = get_ohlcv_with_cache(
        base_dir=OHLCV_DIR,
        loc=loc,
        start_time=request.since,
        count=request.limit or 100,
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
    balance = exchange.fetch_balance(params={})
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
    params: dict = {},
):
    """
    在指定交易所创建订单。
    """
    exchange = exchange_manager.get(exchange_name, market, mode)
    result = exchange.create_order(symbol, type, side, amount, price, params=params)
    return {"order": result}


def create_market_order_ccxt(request: MarketOrderRequest):
    """创建市价订单。"""
    params = request.model_extra or {}
    if request.clientOrderId:
        params["clientOrderId"] = request.clientOrderId

    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="market",
        side=request.side,
        amount=request.amount,
        price=None,
        params=params,
    )


def create_limit_order_ccxt(request: LimitOrderRequest):
    """创建限价订单。"""
    params = request.model_extra or {}
    if request.clientOrderId:
        params["clientOrderId"] = request.clientOrderId
    if request.timeInForce:
        params["timeInForce"] = request.timeInForce
    if request.postOnly:
        params["postOnly"] = request.postOnly

    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="limit",
        side=request.side,
        amount=request.amount,
        price=request.price,
        params=params,
    )


def create_stop_market_order_ccxt(request: StopMarketOrderRequest):
    """创建止损市价订单。"""
    params = {
        "reduceOnly": request.reduceOnly,
        "stopLossPrice": request.triggerPrice,
        **(request.model_extra or {}),
    }

    if request.clientOrderId:
        params["clientOrderId"] = request.clientOrderId
    if request.timeInForce:
        params["timeInForce"] = request.timeInForce

    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="market",
        side=request.side,
        amount=request.amount,
        price=None,
        params=params,
    )


def create_take_profit_market_order_ccxt(request: TakeProfitMarketOrderRequest):
    """创建止盈市价订单。"""
    params = {
        "reduceOnly": request.reduceOnly,
        "takeProfitPrice": request.triggerPrice,
        **(request.model_extra or {}),
    }

    if request.clientOrderId:
        params["clientOrderId"] = request.clientOrderId
    if request.timeInForce:
        params["timeInForce"] = request.timeInForce

    return create_order_ccxt(
        exchange_name=request.exchange_name,
        mode=request.mode,
        market=request.market,
        symbol=request.symbol,
        type="market",
        side=request.side,
        amount=request.amount,
        price=None,
        params=params,
    )


def close_position_ccxt(request: ClosePositionRequest):
    """
    关闭指定品种的当前仓位 (不包含挂单)。

    Equivalent to Close Position.
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = request.symbol

    params = {"reduceOnly": True}
    positions = exchange.fetch_positions([symbol_to_use])

    # Filter positions if side is specified
    if request.side:
        positions = [p for p in positions if p["side"] == request.side]

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

    # Binance Patch
    if request.exchange_name == "binance":
        return binance_adapter.cancel_all_orders(exchange, request)

    result = exchange.cancelAllOrders(request.symbol, params=request.model_extra or {})
    return {"result": result}


def fetch_market_info_ccxt(request: MarketInfoRequest) -> MarketInfoResponse:
    """获取市场信息"""
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    symbol_to_use = request.symbol

    # 1. 获取 market 基础信息
    market = exchange.market(symbol_to_use)

    # 2. 处理 min_amount (若为 None 则回退到 precision)
    min_amount = market["limits"]["amount"]["min"]
    if min_amount is None:
        min_amount = market["precision"]["amount"]

    # 3. 获取当前杠杆 (从 fetch_positions)
    current_leverage = 1  # 默认值
    try:
        positions = exchange.fetch_positions([symbol_to_use])
        if positions:
            pos = positions[0]
            current_leverage = int(pos.get("leverage", 1))
    except Exception as e:
        print(f"Fetch positions failed for {symbol_to_use}: {e}")

    return MarketInfoResponse(
        symbol=request.symbol,
        linear=market.get("linear", False),
        settle=market["settle"],
        precision_amount=float(market["precision"]["amount"]),
        min_amount=float(min_amount),
        contract_size=float(market["contractSize"]),
        leverage=current_leverage,
    )


def fetch_order_ccxt(request: FetchOrderRequest):
    """
    获取特定订单详情
    """
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)

    # Binance Patch
    if request.exchange_name == "binance":
        return binance_adapter.fetch_order(exchange, request)

    result = exchange.fetch_order(id=request.id, symbol=request.symbol, params={})
    return {"order": result}
