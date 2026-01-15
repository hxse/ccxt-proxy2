"""
Research Script: Compare Order Behaviors (Binance vs Kraken)

Tests:
1. Open Orders Visibility (Limit vs Stop)
2. Cancel All Orders Impact (Limit vs Stop)
3. Cancel Single Order Requirements (Params needed?)
4. Closed Orders Visibility (History)

Usage:
    This script will run tests on both exchanges and print results.
"""

import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from debug.utils import get_binance_sandbox, get_kraken_sandbox

# ANSI Colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def log(msg, color=None):
    if color:
        print(f"{color}{msg}{RESET}")
    else:
        print(msg)


def run_test(exchange_name, exchange_factory, symbol):
    log(f"\n{'=' * 20} Testing {exchange_name.upper()} {'=' * 20}", YELLOW)

    try:
        exchange = exchange_factory("future")
    except Exception as e:
        log(f"Failed to init exchange: {e}", RED)
        return

    # Cleanup before start
    try:
        log("Cleaning up existing orders...")
        exchange.cancel_all_orders(symbol)
        if exchange_name == "binance":
            exchange.cancel_all_orders(symbol, params={"stop": True})
    except:
        pass

    results = {}

    try:
        # ==================================================================
        # PHASE 1: Open Orders Visibility
        # ==================================================================
        log("\n[Phase 1] Creating Limit and Stop orders...", YELLOW)

        ticker = exchange.fetch_ticker(symbol)
        price = ticker["last"]

        # Create Limit
        limit_price = round(price * 0.8, 2)
        amount_limit = 0.003
        if exchange_name == "binance":
            amount_limit = round(100 / limit_price, 3) + 0.001

        o_limit = exchange.create_limit_buy_order(symbol, amount_limit, limit_price)
        log(f"Created Limit: {o_limit['id']}")

        # Create Stop
        stop_price = round(price * 0.5, 2)
        params = {"stopLossPrice": stop_price, "reduceOnly": False}
        o_stop = exchange.create_order(symbol, "market", "sell", 0.003, params=params)
        log(f"Created Stop: {o_stop['id']}")

        # Check fetch_open_orders
        log("Fetching open_orders()...")
        open_orders = exchange.fetch_open_orders(symbol)
        ids = [str(o["id"]) for o in open_orders]

        results["open_limit_default"] = str(o_limit["id"]) in ids
        results["open_stop_default"] = str(o_stop["id"]) in ids

        log(
            f"Default Open Orders: Found Limit? {results['open_limit_default']}, Found Stop? {results['open_stop_default']}"
        )

        # Check fetch_open_orders with stop param (Binance specific check mainly)
        log("Fetching open_orders(params={'stop': True})...")
        try:
            open_orders_stop = exchange.fetch_open_orders(symbol, params={"stop": True})
            ids_stop = [str(o["id"]) for o in open_orders_stop]
            results["open_limit_param"] = str(o_limit["id"]) in ids_stop
            results["open_stop_param"] = str(o_stop["id"]) in ids_stop
            log(
                f"Param Open Orders: Found Limit? {results['open_limit_param']}, Found Stop? {results['open_stop_param']}"
            )
        except Exception as e:
            log(f"fetch params failed: {e}")
            results["open_limit_param"] = "Error"
            results["open_stop_param"] = "Error"

        # ==================================================================
        # PHASE 2: Cancel All Orders
        # ==================================================================
        log("\n[Phase 2] Testing cancel_all_orders()...", YELLOW)

        try:
            exchange.cancel_all_orders(symbol)
        except Exception as e:
            log(f"cancel_all_orders failed: {e}", RED)

        remaining = exchange.fetch_open_orders(symbol)
        try:
            remaining_stop = exchange.fetch_open_orders(symbol, params={"stop": True})
        except:
            # If exchange doesn't support this param, assume main fetch covers it or not
            remaining_stop = []

        rem_ids = [str(o["id"]) for o in remaining] + [
            str(o["id"]) for o in remaining_stop
        ]

        results["cancel_all_limit_removed"] = str(o_limit["id"]) not in rem_ids
        results["cancel_all_stop_removed"] = str(o_stop["id"]) not in rem_ids

        log(f"Limit Removed? {results['cancel_all_limit_removed']}")
        log(f"Stop Removed? {results['cancel_all_stop_removed']}")

        # Cleanup for next phase
        try:
            if exchange_name == "binance":
                exchange.cancel_all_orders(symbol, params={"stop": True})
            else:
                exchange.cancel_all_orders(symbol)
        except:
            pass

        # ==================================================================
        # PHASE 3: Single Cancel Order
        # ==================================================================
        log("\n[Phase 3] Testing Single Cancel Order...", YELLOW)

        # Recreate Orders
        o_limit_2 = exchange.create_limit_buy_order(symbol, amount_limit, limit_price)
        o_stop_2 = exchange.create_order(symbol, "market", "sell", 0.003, params=params)

        # Cancel Limit
        try:
            exchange.cancel_order(o_limit_2["id"], symbol)
            results["cancel_single_limit_success"] = True
        except Exception as e:
            log(f"Cancel Limit failed: {e}", RED)
            results["cancel_single_limit_success"] = False

        # Cancel Stop (No params)
        try:
            exchange.cancel_order(o_stop_2["id"], symbol)
            results["cancel_single_stop_noparam_success"] = True
        except Exception as e:
            log(f"Cancel Stop (no param) failed: {e}", RED)
            results["cancel_single_stop_noparam_success"] = False

            # Try with params
            try:
                exchange.cancel_order(o_stop_2["id"], symbol, params={"stop": True})
                results["cancel_single_stop_param_success"] = True
                log("Cancel Stop (with param) success", GREEN)
            except Exception as e2:
                log(f"Cancel Stop (with param) also failed: {e2}", RED)
                results["cancel_single_stop_param_success"] = False

        # ==================================================================
        # PHASE 4: Closed Orders History
        # ==================================================================
        log("\n[Phase 4] Testing Closed Orders History...", YELLOW)

        time.sleep(2)  # Wait for propagation

        closed = exchange.fetch_closed_orders(symbol, limit=50)
        c_ids = [str(o["id"]) for o in closed]

        results["closed_limit_found"] = (
            str(o_limit_2["id"]) in c_ids or str(o_limit["id"]) in c_ids
        )
        results["closed_stop_found"] = (
            str(o_stop_2["id"]) in c_ids or str(o_stop["id"]) in c_ids
        )

        log(f"Closed Limit Found? {results['closed_limit_found']}")
        log(f"Closed Stop Found? {results['closed_stop_found']}")

        if not results["closed_stop_found"]:
            try:
                closed_stop = exchange.fetch_closed_orders(
                    symbol, limit=50, params={"stop": True}
                )
                cs_ids = [str(o["id"]) for o in closed_stop]
                results["closed_stop_param_found"] = (
                    str(o_stop_2["id"]) in cs_ids or str(o_stop["id"]) in cs_ids
                )
                log(f"Closed Stop (param) Found? {results['closed_stop_param_found']}")
            except Exception as e:
                log(f"fetch closed params failed: {e}")
                results["closed_stop_param_found"] = "Error"
        else:
            results["closed_stop_param_found"] = "N/A (Found in default)"

        # ==================================================================
        # PHASE 5: Fetch Single Order
        # ==================================================================
        log("\n[Phase 5] Testing Fetch Single Order...", YELLOW)

        # Create fresh orders
        o_limit_3 = exchange.create_limit_buy_order(symbol, amount_limit, limit_price)
        params = {"stopLossPrice": stop_price, "reduceOnly": False}
        o_stop_3 = exchange.create_order(symbol, "market", "sell", 0.003, params=params)

        # Fetch Limit
        try:
            f_limit = exchange.fetch_order(o_limit_3["id"], symbol)
            results["fetch_single_limit_success"] = True
        except Exception as e:
            log(f"Fetch Limit failed: {e}", RED)
            results["fetch_single_limit_success"] = False

        # Fetch Stop (Default)
        try:
            f_stop = exchange.fetch_order(o_stop_3["id"], symbol)
            results["fetch_single_stop_default_success"] = True
            log("Fetch Stop (default) success", GREEN)
        except Exception as e:
            log(f"Fetch Stop (default) failed: {e}", RED)
            results["fetch_single_stop_default_success"] = False

            # Fetch Stop (Param)
            try:
                f_stop_p = exchange.fetch_order(
                    o_stop_3["id"], symbol, params={"stop": True}
                )
                results["fetch_single_stop_param_success"] = True
                log("Fetch Stop (param) success", GREEN)
            except Exception as e2:
                log(f"Fetch Stop (param) failed: {e2}", RED)
                results["fetch_single_stop_param_success"] = False

        # Cleanup
        try:
            exchange.cancel_all_orders(symbol)
            if exchange_name == "binance":
                exchange.cancel_all_orders(symbol, params={"stop": True})
        except:
            pass

    except Exception as e:
        log(f"CRITICAL ERROR IN TEST: {e}", RED)
        import traceback

        traceback.print_exc()

    return results


def main():
    print("Starting Research...")

    # Binance
    res_binance = run_test("binance", get_binance_sandbox, "BTC/USDT:USDT")

    # Kraken
    # Note: Check Kraken symbol format
    # res_kraken = run_test("kraken", get_kraken_sandbox, "BTC/USD:USD")
    res_kraken = {}  # Skip Kraken for now to save time/errors

    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)

    print(f"{'Metric':<40} | {'Binance':<10} | {'Kraken':<10}")
    print("-" * 66)

    keys = [
        "open_limit_default",
        "open_stop_default",
        "open_limit_param",
        "open_stop_param",
        "cancel_all_limit_removed",
        "cancel_all_stop_removed",
        "cancel_single_limit_success",
        "cancel_single_stop_noparam_success",
        "cancel_single_stop_param_success",
        "closed_limit_found",
        "closed_stop_found",
        "closed_stop_param_found",
        "fetch_single_limit_success",
        "fetch_single_stop_default_success",
        "fetch_single_stop_param_success",
    ]

    for k in keys:
        b_val = res_binance.get(k, "SKIP") if res_binance else "ERR"
        k_val = res_kraken.get(k, "SKIP") if res_kraken else "ERR"
        print(f"{k:<40} | {str(b_val):<10} | {str(k_val):<10}")


if __name__ == "__main__":
    main()
