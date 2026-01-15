from src.tools.exchange_manager import exchange_manager
from src.tools import binance_adapter
from src.types_extended import (
    FetchOpenOrdersRequest,
    FetchClosedOrdersRequest,
    FetchMyTradesRequest,
    FetchPositionsRequest,
    SetLeverageRequest,
    SetMarginModeRequest,
    CancelOrderRequest,
)


def fetch_open_orders_ccxt(request: FetchOpenOrdersRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)

    # Binance Patch
    if request.exchange_name == "binance":
        return binance_adapter.fetch_open_orders(exchange, request)

    orders = exchange.fetch_open_orders(
        symbol=request.symbol,
        since=request.since,
        limit=request.limit,
        params={},
    )
    return {"orders": orders}


def fetch_closed_orders_ccxt(request: FetchClosedOrdersRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)

    # Binance Patch
    if request.exchange_name == "binance":
        return binance_adapter.fetch_closed_orders(exchange, request)

    orders = exchange.fetch_closed_orders(
        symbol=request.symbol,
        since=request.since,
        limit=request.limit,
        params={},
    )
    return {"orders": orders}


def fetch_my_trades_ccxt(request: FetchMyTradesRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    trades = exchange.fetch_my_trades(
        symbol=request.symbol,
        since=request.since,
        limit=request.limit,
        params={},
    )
    return {"trades": trades}


def fetch_positions_ccxt(request: FetchPositionsRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    positions = exchange.fetch_positions(
        symbols=request.symbols,
        params={},
    )
    return {"positions": positions}


def set_leverage_ccxt(request: SetLeverageRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    # setLeverage(leverage, symbol=None, params={})
    # Note: symbol is practically required for most exchanges
    result = exchange.set_leverage(
        leverage=request.leverage,
        symbol=request.symbol,
        params=request.model_extra or {},
    )
    return {"result": result}


def set_margin_mode_ccxt(request: SetMarginModeRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)
    result = exchange.set_margin_mode(
        marginMode=request.marginMode,
        symbol=request.symbol,
        params=request.model_extra or {},
    )
    return {"result": result}


def cancel_order_ccxt(request: CancelOrderRequest):
    exchange = exchange_manager.get(request.exchange_name, request.market, request.mode)

    # Binance Patch
    if request.exchange_name == "binance":
        return binance_adapter.cancel_order(exchange, request)

    order = exchange.cancel_order(
        id=request.id,
        symbol=request.symbol,
        params=request.model_extra or {},
    )
    return {"order": order}
