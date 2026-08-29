import sys
import os
import json
import traceback
from datetime import datetime

# Allow importing from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_debug_client


def analyze_type(value, prefix=""):
    """Recursively analyze and print types of a value"""
    if value is None:
        print(f"{prefix}: None (Type: NoneType)")
        return

    if isinstance(value, dict):
        # Print a few example keys if too many
        pass

    print(f"{prefix}: {value} (Type: {type(value).__name__})")


def print_section(name):
    print(f"\n{'=' * 60}")
    print(f"VERIFYING: {name}")
    print(f"{'=' * 60}")


def main():
    try:
        client = get_debug_client("binance", "future", "sandbox")
        symbol_request = "BTC/USDT:USDT"
        print(f"Using Symbol: {symbol_request}")

        # 1. Market Info
        print_section("Market Info")
        try:
            market = client.fetch_market_info(symbol_request)
            for key, value in market.items():
                analyze_type(value, key)
        except Exception:
            traceback.print_exc()

        # 2. Tickers
        print_section("Tickers")
        try:
            # fetch_tickers might require List[str]
            tickers = client.fetch_tickers([symbol_request])
            if tickers:
                # Use the first ticker found
                t = list(tickers.values())[0]
                print(f"Sample Ticker Key: {list(tickers.keys())[0]}")

                keys_to_check = [
                    "symbol",
                    "timestamp",
                    "datetime",
                    "high",
                    "low",
                    "bid",
                    "bidVolume",
                    "ask",
                    "askVolume",
                    "vwap",
                    "open",
                    "close",
                    "last",
                    "previousClose",
                    "change",
                    "percentage",
                    "average",
                    "baseVolume",
                    "quoteVolume",
                ]
                for k in keys_to_check:
                    analyze_type(t.get(k), k)
            else:
                print("No tickers returned")
        except Exception:
            traceback.print_exc()

        # 3. Balance
        print_section("Balance")
        try:
            balance = client.fetch_balance()
            if "free" in balance and balance["free"]:
                first_currency = list(balance["free"].keys())[0]
                analyze_type(
                    balance["free"][first_currency], f"free['{first_currency}']"
                )
                analyze_type(
                    balance["used"][first_currency], f"used['{first_currency}']"
                )
                analyze_type(
                    balance["total"][first_currency], f"total['{first_currency}']"
                )
        except Exception:
            traceback.print_exc()

        # 4. Order
        print_section("Order")
        try:
            # Try fetching closed orders as it's safer
            orders = client.fetch_closed_orders(symbol_request, None, 1)
            if not orders:
                orders = client.fetch_open_orders(symbol_request, None, None)

            if orders:
                o = orders[0]
                keys = [
                    "id",
                    "clientOrderId",
                    "datetime",
                    "timestamp",
                    "lastTradeTimestamp",
                    "status",
                    "symbol",
                    "type",
                    "side",
                    "price",
                    "amount",
                    "filled",
                    "remaining",
                    "cost",
                    "average",
                    "fee",
                ]
                for k in keys:
                    analyze_type(o.get(k), k)
            else:
                print("No orders found to verify.")
        except Exception:
            traceback.print_exc()

        # 5. OHLCV
        print_section("OHLCV")
        try:
            ohlcv = client.fetch_ohlcv_latest_limit(
                symbol_request, "1m", 1, enable_cache=False
            ).rows
            if ohlcv:
                item = ohlcv[0]
                labels = ["Time", "Open", "High", "Low", "Close", "Volume"]
                for i, val in enumerate(item):
                    analyze_type(val, labels[i])
        except Exception:
            traceback.print_exc()

        # 6. Position (fetch_positions)
        print_section("Position")
        try:
            positions = client.fetch_positions([symbol_request])
            if positions:
                p = positions[0]
                keys = [
                    "symbol",
                    "timestamp",
                    "datetime",
                    "contracts",
                    "contractSize",
                    "side",
                    "notional",
                    "leverage",
                    "collateral",
                    "entryPrice",
                    "markPrice",
                    "liquidationPrice",
                    "hedged",
                    "unrealizedPnl",
                    "percentage",
                    "maintenanceMargin",
                    "initialMargin",
                    "marginRatio",
                    "marginMode",
                ]
                for k in keys:
                    analyze_type(p.get(k), k)
            else:
                print("No positions found")
        except Exception:
            traceback.print_exc()

    except Exception:
        print("CRITICAL MAIN ERROR")
        traceback.print_exc()


if __name__ == "__main__":
    main()
