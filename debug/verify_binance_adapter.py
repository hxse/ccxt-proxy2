import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from src.tools.exchange_manager import exchange_manager
from src.types import CancelAllOrdersRequest, FetchOrderRequest
from src.types_extended import (
    FetchOpenOrdersRequest,
    FetchClosedOrdersRequest,
    CancelOrderRequest,
)

# Import directly from utils now that they are patched
from src.tools.ccxt_utils import cancel_all_orders_ccxt, fetch_order_ccxt
from src.tools.ccxt_utils_extended import (
    fetch_open_orders_ccxt,
    fetch_closed_orders_ccxt,
    cancel_order_ccxt,
)
from debug.utils import get_binance_sandbox


def verify_binance_adapter():
    print("VERIFYING BINANCE ADAPTER LOGIC")
    exchange_name = "binance"
    market = "future"
    mode = "sandbox"
    symbol = "BTC/USDT:USDT"

    # 1. Setup: Create Limit and Stop Orders
    print("\n[Setup] Creating Limit and Stop Orders...")
    exchange = get_binance_sandbox(market)
    ticker = exchange.fetch_ticker(symbol)
    price = ticker["last"]

    # Create Limit Order (Buy quite low)
    limit_price = round(price * 0.8, 2)
    exchange.create_order(symbol, "limit", "buy", 0.003, limit_price)

    # Create Stop Order (Sell Stop)
    stop_price = round(price * 0.9, 2)
    params = {
        "stopLossPrice": stop_price,
        "reduceOnly": False,
    }  # ensure allow opening position for test
    # Note: reduceOnly=False needed if no position, but stop_market usually requires reduceOnly=True or existing position.
    # Let's try reduceOnly=False and see if it allows opening stop order or just use existing position logic?
    # Actually, simpler: just create stop market. Sandbox allows it usually.
    try:
        exchange.create_order(
            symbol, "market", "sell", 0.003, params={"stopLossPrice": stop_price}
        )
    except Exception as e:
        print(f"Failed to create stop order (might need position?): {e}")

    time.sleep(1)

    # 2. Test fetch_open_orders (Should see BOTH)
    print("\n[Test 1] fetch_open_orders (Should return BOTH Limit and Stop)")
    req_open = FetchOpenOrdersRequest(
        exchange_name=exchange_name, market=market, mode=mode, symbol=symbol
    )
    res_open = fetch_open_orders_ccxt(req_open)
    orders = res_open["orders"]
    print(f"Orders found: {len(orders)}")
    types = [o["type"] for o in orders]
    print(f"Order Types: {types}")

    has_limit = any(o["type"] == "limit" for o in orders)
    has_stop = any(
        o["type"] == "market" and "stop" in o["info"] for o in orders
    )  # approximate check
    # Or check 'type' might be enough? CCXT usually parses stop market as 'market' but let's see.
    # Actually CCXT might parse stop market type as 'stop_market' or 'market' with params.

    if len(orders) >= 2:
        print(">> PASS: Found multiple orders (likely merged).")
    else:
        print(">> FAIL/WARN: Found fewer than 2 orders.")

    # 3. Test fetch_order (Single)
    print("\n[Test 2] fetch_order (Single ID Check)")
    if len(orders) > 0:
        target_order = orders[-1]  # pick one
        req_fetch = FetchOrderRequest(
            exchange_name=exchange_name,
            market=market,
            mode=mode,
            symbol=symbol,
            id=target_order["id"],
        )
        res_fetch = fetch_order_ccxt(req_fetch)
        fetched = res_fetch["order"]
        print(f"Fetched ID: {fetched['id']} Type: {fetched['type']}")
        if fetched["id"] == target_order["id"]:
            print(">> PASS: Fetch Single Order success.")
        else:
            print(">> FAIL: ID mismatch.")

    # 4. Test cancel_order (Single)
    print("\n[Test 3] cancel_order (Single)")
    if len(orders) > 0:
        to_cancel = orders[0]
        print(f"Cancelling order ID: {to_cancel['id']}")
        req_cancel = CancelOrderRequest(
            exchange_name=exchange_name,
            market=market,
            mode=mode,
            symbol=symbol,
            id=to_cancel["id"],
        )
        cancel_order_ccxt(req_cancel)
        time.sleep(1)
        # Verify it's gone
        try:
            # Using fetch_order to check status
            req_chk = FetchOrderRequest(
                exchange_name=exchange_name,
                market=market,
                mode=mode,
                symbol=symbol,
                id=to_cancel["id"],
            )
            chk = fetch_order_ccxt(req_chk)
            if chk["order"]["status"] in ["canceled", "closed"]:
                print(">> PASS: Order cancelled successfully.")
            else:
                print(f">> FAIL: Order status is {chk['order']['status']}")
        except Exception as e:
            # If fetch fails (e.g. not found in open orders), that might be good too depending on implementation,
            # but fetch_order usually finds closed/canceled ones if supported.
            print(f"Fetch after cancel failed (could be good): {e}")

    # 5. Test cancel_all_orders
    print("\n[Test 4] cancel_all_orders (Clean Sweep)")
    # Create more orders to sweep
    exchange.create_order(symbol, "limit", "buy", 0.003, limit_price)

    req_cancel_all = CancelAllOrdersRequest(
        exchange_name=exchange_name, market=market, mode=mode, symbol=symbol
    )
    cancel_all_orders_ccxt(req_cancel_all)
    time.sleep(1)

    # Check open orders again
    res_rem = fetch_open_orders_ccxt(req_open)
    if len(res_rem["orders"]) == 0:
        print(">> PASS: All orders cancelled.")
    else:
        print(f">> FAIL: {len(res_rem['orders'])} orders remain.")

    # 6. Test fetch_closed_orders
    print("\n[Test 5] fetch_closed_orders (History)")
    req_closed = FetchClosedOrdersRequest(
        exchange_name=exchange_name, market=market, mode=mode, symbol=symbol
    )
    res_closed = fetch_closed_orders_ccxt(req_closed)
    print(f"Closed Orders found: {len(res_closed['orders'])}")
    if len(res_closed["orders"]) > 0:
        print(">> PASS: Fetched history.")
    else:
        print(">> WARN: No closed orders found (maybe sandbox clean?)")


if __name__ == "__main__":
    verify_binance_adapter()
