import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from debug.utils import get_debug_client
from src.base_types import ExchangeName, MarketType


def cleanup(client, symbol):
    print(f"Cleaning up {symbol} on {client.exchange_name}...")
    try:
        orders = client.cancel_all_orders(symbol)
        print(
            f"  Cancelled {len(orders) if isinstance(orders, list) else 'all'} orders."
        )
    except Exception as e:
        print(f"  Error cancelling orders: {e}")

    try:
        remaining = client.close_position(symbol)
        print(f"  Remaining positions: {len(remaining)}")
    except Exception as e:
        print(f"  Error closing positions: {e}")


def main():
    targets: list[tuple[ExchangeName, MarketType, str]] = [
        ("binance", "future", "BTC/USDT:USDT"),
        ("kraken", "future", "BTC/USD:USD"),
    ]

    for exchange_name, market, symbol in targets:
        try:
            client = get_debug_client(exchange_name, market, "sandbox")
            cleanup(client, symbol)
        except Exception as e:
            print(f"Failed to setup {exchange_name} {market}: {e}")


if __name__ == "__main__":
    main()
