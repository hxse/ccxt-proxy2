"""
Research Script: Verify behavior of /ccxt/close_all_orders logic

Hypothesis:
The current implementation of `close_all_order_ccxt` only closes positions (via `reduceOnly` market orders)
but DOES NOT cancel existing open orders (Limit or Stop).

Test Flow:
1. Open a Position (Buy Market).
2. Create Open Orders (Limit & Stop).
3. Call `close_all_order_ccxt` logic (simulate via `exchange_manager` or direct call).
4. Verify:
   - Position is closed (contracts == 0).
   - Open Orders (Limit & Stop) are STILL THERE (or cancelled?).
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from debug.utils import get_kraken_sandbox


def main():
    print("=" * 60)
    print("Research: Close All Orders Logic")
    print("=" * 60)

    exchange = get_kraken_sandbox("future")
    symbol = "BTC/USD:USD"

    # Cleanup
    try:
        exchange.cancel_all_orders(symbol)
        exchange.cancel_all_orders(symbol, params={"stop": True})
        # Close any existing pos
        positions = exchange.fetch_positions([symbol])
        for p in positions:
            if p["contracts"] > 0:
                side = "sell" if p["side"] == "long" else "buy"
                exchange.create_order(
                    symbol, "market", side, p["contracts"], params={"reduceOnly": True}
                )
    except:
        pass

    # 1. Setup Position & Orders
    print("\n[1] Setting up Position & Orders...")
    try:
        # Open Position
        print("Opening Position: Buy 0.003 BTC")
        exchange.create_order(symbol, "market", "buy", 0.003)

        ticker = exchange.fetch_ticker(symbol)
        price = ticker["last"]

        # Create Limit
        limit_price = round(price * 0.8, 2)
        amount_limit = round(100 / limit_price, 3) + 0.001
        print(f"Creating Limit: {limit_price}")
        exchange.create_limit_buy_order(symbol, amount_limit, limit_price)

        # Create Stop
        stop_price = round(price * 0.5, 2)
        params = {"stopLossPrice": stop_price, "reduceOnly": False}
        print(f"Creating Stop: {stop_price}")
        exchange.create_order(symbol, "market", "sell", 0.003, params=params)

    except Exception as e:
        print(f"Setup failed: {e}")
        return

    # 2. Execute Close All Logic (Copy-paste from src/tools/ccxt_utils.py)
    print("\n[2] Executing close_all_order_ccxt logic...")
    try:
        # --- Logic from ccxt_utils.py ---
        params = {"reduceOnly": True}
        positions = exchange.fetch_positions([symbol])
        print(f"Current Position: {positions[0]['contracts']} contracts")

        for i in positions:
            if i["contracts"] > 0:
                side = "sell" if i["side"] == "long" else "buy"
                amount = i["contracts"]
                print(f"Closing position: {side} {amount}")
                exchange.create_order(symbol, "market", side, amount, params=params)
            else:
                print("No open position to close.")
        # --------------------------------

    except Exception as e:
        print(f"Close logic failed: {e}")

    # 3. Verify Result
    print("\n[3] Verifying Result...")

    # Check Position
    positions = exchange.fetch_positions([symbol])
    pos_size = positions[0]["contracts"] if positions else 0
    print(f"Final Position Size: {pos_size}")
    if pos_size == 0:
        print(">> Position Closed ✅")
    else:
        print(">> Position NOT Closed ❌")

    # Check Orders
    orders = exchange.fetch_open_orders(symbol)
    if len(orders) > 0:
        print(
            f">> Limit Orders Remaining: {len(orders)} (Expected: >0 if logic doesn't cancel) ⚠️"
        )
    else:
        print(">> Limit Orders Cleared (Unexpected) ❓")

    try:
        orders_stop = exchange.fetch_open_orders(symbol, params={"stop": True})
        if len(orders_stop) > 0:
            print(f">> Stop Orders Remaining: {len(orders_stop)} (Expected: >0) ⚠️")
        else:
            print(">> Stop Orders Cleared (Unexpected) ❓")
    except:
        pass

    print("\n[Conclusion]")
    if pos_size == 0 and (len(orders) > 0 or len(orders_stop) > 0):
        print("Logic ONLY closes positions. Open/Stop orders remain active.")
        print("Risk: If price moves, remaining orders might open NEW positions.")

    with open("debug/result.txt", "w", encoding="utf-8") as f:
        f.write(f"Final Position Size: {pos_size}\n")
        f.write(f"Limit Orders Remaining: {len(orders)}\n")
        try:
            f.write(f"Stop Orders Remaining: {len(orders_stop)}\n")
        except:
            f.write("Stop Orders Remaining: Unknown\n")

        if pos_size == 0 and len(orders) == 0 and len(orders_stop) == 0:
            f.write("CONCLUSION: CLEAN_CLOSE (All Cancelled)\n")
        elif pos_size == 0:
            f.write("CONCLUSION: POS_CLOSED_BUT_ORDERS_REMAIN\n")


if __name__ == "__main__":
    main()
