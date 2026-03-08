import ccxt
from loguru import logger
from src.types import CancelAllOrdersRequest, FetchOrderRequest
from src.types_extended import (
    FetchOpenOrdersRequest,
    FetchClosedOrdersRequest,
    CancelOrderRequest,
)


# --- Fetch Open Orders ---
def fetch_open_orders(exchange, request: FetchOpenOrdersRequest):
    """
    Patched fetch_open_orders for Binance:
    Merges Limit orders (default) and Stop orders (params={'stop': True}).
    """
    # 1. Fetch Limit Orders
    limit_orders = exchange.fetch_open_orders(
        symbol=request.symbol, since=request.since, limit=request.limit, params={}
    )

    # 2. Fetch Stop Orders
    stop_orders = exchange.fetch_open_orders(
        symbol=request.symbol,
        since=request.since,
        limit=request.limit,
        params={"stop": True},
    )

    # 3. Merge and Sort
    all_orders = limit_orders + stop_orders
    # Sort by timestamp descending
    all_orders.sort(key=lambda x: x["timestamp"], reverse=True)

    return {"orders": all_orders}


# --- Fetch Closed Orders ---
def fetch_closed_orders(exchange, request: FetchClosedOrdersRequest):
    """
    Patched fetch_closed_orders for Binance:
    Merges Limit orders (default) and Stop orders (params={'stop': True}).
    """
    # 1. Fetch Limit History
    limit_orders = exchange.fetch_closed_orders(
        symbol=request.symbol, since=request.since, limit=request.limit, params={}
    )

    # 2. Fetch Stop History
    stop_orders = exchange.fetch_closed_orders(
        symbol=request.symbol,
        since=request.since,
        limit=request.limit,
        params={"stop": True},
    )

    # 3. Merge and Sort
    all_orders = limit_orders + stop_orders
    all_orders.sort(key=lambda x: x["timestamp"], reverse=True)

    return {"orders": all_orders}


# --- Cancel All Orders ---
def cancel_all_orders(exchange, request: CancelAllOrdersRequest):
    """
    Patched cancel_all_orders for Binance:
    Cancels Limit orders (default) AND Stop orders (params={'stop': True}).
    """
    results = []
    bound_logger = logger.bind(exchange="binance", symbol=request.symbol)

    # 1. Cancel Limit Orders
    bound_logger.info("cancelling limit orders")
    res_limit = exchange.cancel_all_orders(request.symbol, params={})
    bound_logger.info(
        "limit order cancellation completed: {}",
        len(res_limit) if isinstance(res_limit, list) else res_limit,
    )
    results.append(res_limit)

    # 2. Cancel Stop Orders
    bound_logger.info("cancelling stop orders")
    res_stop = exchange.cancel_all_orders(request.symbol, params={"stop": True})
    bound_logger.info(
        "stop order cancellation completed: {}",
        len(res_stop) if isinstance(res_stop, list) else res_stop,
    )
    results.append(res_stop)

    return {"result": results}


# --- Fetch Single Order ---
def fetch_order(exchange, request: FetchOrderRequest):
    """
    Patched fetch_order for Binance:
    Tries default fetch. If fails with 'Order does not exist', retries with params={'stop': True}.
    """
    try:
        return {
            "order": exchange.fetch_order(
                id=request.id, symbol=request.symbol, params={}
            )
        }
    except ccxt.OrderNotFound:
        # Retry with stop param
        return {
            "order": exchange.fetch_order(
                id=request.id, symbol=request.symbol, params={"stop": True}
            )
        }


# --- Cancel Single Order ---
def cancel_order(exchange, request: CancelOrderRequest):
    """
    Patched cancel_order for Binance:
    Tries default cancel. If fails with 'Unknown order', retries with params={'stop': True}.
    """
    bound_logger = logger.bind(
        exchange="binance", symbol=request.symbol, order_id=request.id
    )
    try:
        bound_logger.info("cancelling order with default params")
        res = exchange.cancel_order(id=request.id, symbol=request.symbol, params={})
        bound_logger.info(
            "default order cancellation succeeded: {}",
            res.get("status", "unknown"),
        )
        return {"order": res}
    except ccxt.OrderNotFound as e:
        # Binance often throws "Unknown order sent" (code -2011)
        # or "Order does not exist" (code -2013)
        bound_logger.warning("default order cancellation failed: {}", e)

        # Try to cancel as stop order
        bound_logger.info("retrying order cancellation with stop params")
        res_stop = exchange.cancel_order(
            id=request.id, symbol=request.symbol, params={"stop": True}
        )
        bound_logger.info(
            "stop order cancellation succeeded: {}",
            res_stop.get("status", "unknown"),
        )
        return {"order": res_stop}
