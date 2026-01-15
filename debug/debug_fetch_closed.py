import sys
import os
import time
from pprint import pprint

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tools.exchange_manager import exchange_manager
from src.tools.ccxt_utils import (
    fetch_tickers_ccxt,
    create_limit_order_ccxt,
)
from src.tools.ccxt_utils_extended import (
    fetch_closed_orders_ccxt,
    cancel_order_ccxt,
)
from src.types import TickersRequest, LimitOrderRequest
from src.types_extended import FetchClosedOrdersRequest, CancelOrderRequest

EXCHANGE = "binance"
MARKET = "future"
MODE = "sandbox"
SYMBOL = "BTC/USDT:USDT"


def debug_fetch_closed():
    print("--- Debugging fetch_closed_orders ---")

    # 1. Create a Limit Order
    print("[1] Creating new Limit Order...")
    # Get price
    ticker = fetch_tickers_ccxt(
        TickersRequest(exchange_name=EXCHANGE, market=MARKET, mode=MODE, symbols=SYMBOL)
    )
    price = ticker["tickers"][SYMBOL]["last"]
    limit_price = round(price * 0.5, 2)  # Deep OTM

    req_l = LimitOrderRequest(
        exchange_name=EXCHANGE,
        market=MARKET,
        mode=MODE,
        symbol=SYMBOL,
        side="buy",
        amount=0.005,
        price=limit_price,
    )
    res_l = create_limit_order_ccxt(req_l)
    order_id = res_l["order"]["id"]
    print(f"Created Order: {order_id}")

    time.sleep(1)

    # 2. Cancel it
    print(f"[2] Cancelling Order: {order_id}")
    req_c = CancelOrderRequest(
        exchange_name=EXCHANGE, market=MARKET, mode=MODE, symbol=SYMBOL, id=order_id
    )
    cancel_order_ccxt(req_c)

    time.sleep(2)  # Wait for propagation

    # 3. Fetch Closed Orders (Try multiple times)
    print("[3] Fetching Closed Orders...")

    for i in range(3):
        print(f"\nAttempt {i + 1}:")
        req_cl = FetchClosedOrdersRequest(
            exchange_name=EXCHANGE, market=MARKET, mode=MODE, symbol=SYMBOL
        )  # Default limit
        res_cl = fetch_closed_orders_ccxt(req_cl)
        orders = res_cl["orders"]
        print(f"Fetched {len(orders)} closed orders.")

        # Check for our ID
        found = next((o for o in orders if o["id"] == order_id), None)

        if found:
            print(f">> FOUND! Status: {found['status']}")
            break
        else:
            print(">> NOT FOUND in this batch.")
            ids = [o["id"] for o in orders]
            print(f"Top 5 IDs: {ids[:5]}")
            print(f"timestamps: {[o['timestamp'] for o in orders[:5]]}")

        time.sleep(2)


if __name__ == "__main__":
    try:
        debug_fetch_closed()
    except Exception as e:
        print(f"CRASH: {e}")
        import traceback

        traceback.print_exc()
