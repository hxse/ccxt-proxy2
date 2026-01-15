import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from debug.utils import get_binance_sandbox, get_kraken_sandbox
import time


def cleanup(exchange, symbol):
    print(f"Cleaning up {symbol} on {exchange.id}...")
    try:
        # Cancel all open orders
        orders = exchange.cancel_all_orders(symbol)
        print(
            f"  Cancelled {len(orders) if isinstance(orders, list) else 'all'} orders."
        )
    except Exception as e:
        print(f"  Error cancelling orders: {e}")

    try:
        # Close positions
        # For simple cleanup, we just check positions and limit close
        # Note: Sandbox might behave differently, simple market close is best
        positions = exchange.fetch_positions([symbol])
        for pos in positions:
            size = float(pos["contracts"])
            if size > 0:
                side = "sell" if pos["side"] == "long" else "buy"
                print(f"  Closing position: {pos['side']} {size}")
                try:
                    exchange.create_order(symbol, "market", side, size)
                    print("    Success.")
                except Exception as e:
                    print(f"    Failed to close: {e}")
            else:
                print("  No open position.")

    except Exception as e:
        print(f"  Error checking positions: {e}")


def main():
    # Define targets
    targets = [
        ("binance", "future", "BTC/USDT:USDT"),
        ("binance", "delivery", "BTC/USD:BTC"),
        # Kraken sandbox position fetching might be tricky, but we try
        ("kraken", "future", "BTC/USD:USD"),
        ("kraken", "delivery", "BTC/USD:BTC"),
    ]

    for ex_name, mode, symbol in targets:
        try:
            if ex_name == "binance":
                ex = get_binance_sandbox(mode)
            else:
                ex = get_kraken_sandbox(mode)

            cleanup(ex, symbol)

        except Exception as e:
            print(f"Failed to setup {ex_name} {mode}: {e}")


if __name__ == "__main__":
    main()
