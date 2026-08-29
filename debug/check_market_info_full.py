import sys
import os

# Allow importing from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_debug_client, print_json


def check_symbol(client, symbol):
    print(f"\n{'=' * 40}")
    print(f"Checking {symbol}")
    print(f"{'=' * 40}")

    try:
        print_json(client.fetch_market_info(symbol))
    except Exception as e:
        print(f"Error checking {symbol}: {e}")


def main():
    try:
        client = get_debug_client("binance", "future", "sandbox")
        check_symbol(client, "BTC/USDT:USDT")
        check_symbol(client, "ETH/USDT:USDT")

    except Exception as e:
        print(f"Global Error: {e}")


if __name__ == "__main__":
    main()
