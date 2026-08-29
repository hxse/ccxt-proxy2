import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from debug.utils import get_debug_client, print_json


def test_leverage(client, symbol, label):
    print(f"\n{'=' * 50}")
    print(f"Testing {label} - {symbol}")
    print(f"{'=' * 50}")

    try:
        print_json(client.fetch_market_info(symbol))
    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    try:
        client = get_debug_client("binance", "future", "sandbox")
        test_leverage(client, "BTC/USDT:USDT", "Binance USDⓈ-M linear")
    except Exception as e:
        print(f"Binance Init Error: {e}")

    try:
        client = get_debug_client("kraken", "future", "sandbox")
        test_leverage(client, "BTC/USD:USD", "Kraken Futures linear")
    except Exception as e:
        print(f"Kraken Future Init Error: {e}")

    try:
        client = get_debug_client("kraken", "future", "sandbox")
        test_leverage(client, "BTC/USD:BTC", "Kraken Futures inverse")
    except Exception as e:
        print(f"Kraken Delivery Init Error: {e}")


class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


if __name__ == "__main__":
    sys.stdout = Logger("debug/debug_leverage.log")
    main()
