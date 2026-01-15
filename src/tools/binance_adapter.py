import ccxt
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

    # 1. Cancel Limit Orders
    print(f"[BinanceAdapter] Cancelling Limit Orders for {request.symbol}...")
    res_limit = exchange.cancel_all_orders(request.symbol, params={})
    print(
        f"[BinanceAdapter] Limit Cancel Result: {len(res_limit) if isinstance(res_limit, list) else res_limit}"
    )
    results.append(res_limit)

    # 2. Cancel Stop Orders
    print(f"[BinanceAdapter] Cancelling Stop Orders for {request.symbol}...")
    res_stop = exchange.cancel_all_orders(request.symbol, params={"stop": True})
    print(
        f"[BinanceAdapter] Stop Cancel Result: {len(res_stop) if isinstance(res_stop, list) else res_stop}"
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
    try:
        print(f"[BinanceAdapter] Cancelling Order ID {request.id} (Default)...")
        res = exchange.cancel_order(id=request.id, symbol=request.symbol, params={})
        print(
            f"[BinanceAdapter] Default Cancel Success: {res.get('status', 'unknown')}"
        )
        return {"order": res}
    except ccxt.OrderNotFound as e:
        # Binance often throws "Unknown order sent" (code -2011)
        # or "Order does not exist" (code -2013)
        print(f"[BinanceAdapter] Default Cancel Failed: {e}")

        # Try to cancel as stop order
        print(
            f"[BinanceAdapter] Retrying Cancel Order ID {request.id} with Stop param..."
        )
        res_stop = exchange.cancel_order(
            id=request.id, symbol=request.symbol, params={"stop": True}
        )
        print(
            f"[BinanceAdapter] Stop Cancel Success: {res_stop.get('status', 'unknown')}"
        )
        return {"order": res_stop}
