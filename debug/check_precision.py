import sys
import os

# Allow importing from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_debug_client, print_json


def main():
    try:
        client = get_debug_client("binance", "future", "sandbox")
        symbol = "BTC/USDT:USDT"

        print(f"Fetching market info for {symbol}...")
        print_json(client.fetch_market_info(symbol))
        print_json(client.fetch_market_info("ETH/USDT:USDT"))

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
