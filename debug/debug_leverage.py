import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

from debug.utils import get_binance_sandbox, get_kraken_sandbox, print_json
import ccxt


def test_leverage(exchange, symbol, label):
    print(f"\n{'=' * 50}")
    print(f"Testing {label} - {symbol}")
    print(f"{'=' * 50}")

    try:
        # 1. Fetch Positions (current leverage)
        print("1. Fetching Positions (Current Leverage)...")
        positions = exchange.fetch_positions([symbol])

        current_leverage = "Not Found"
        if positions:
            # Usually leverage is in the first position for the symbol
            pos = positions[0]
            current_leverage = pos.get("leverage", "N/A")
            print(f"✅ Found Position: Leverage = {current_leverage}")
            print(
                f"   Raw Info: mode={pos.get('marginMode')}, side={pos.get('side')}, amount={pos.get('contracts')}"
            )
        else:
            print(
                "⚠️ No open positions found (Expected if account is empty, but API call worked)"
            )

        # 2. Fetch Leverage Tiers (Max leverage configs)
        print("\n2. Fetching Leverage Tiers (Config)...")
        try:
            tiers = exchange.fetch_leverage_tiers([symbol])
            if symbol in tiers:
                tier_list = tiers[symbol]
                # Show first and last tier to be concise
                if tier_list:
                    print(f"✅ Found {len(tier_list)} Tiers.")
                    first_tier = tier_list[0]
                    # print(f"   Keys: {list(first_tier.keys())}")

                    min_lev = first_tier.get(
                        "minLeverage", 1
                    )  # Default to 1 if missing
                    max_lev = first_tier.get("maxLeverage", "N/A")

                    print(f"   Min Leverage: {min_lev}")
                    print(f"   Max Leverage: {max_lev}")
            else:
                print(f"⚠️ No tiers data for {symbol}")
        except Exception as e:
            print(f"❌ fetch_leverage_tiers failed: {e}")

    except Exception as e:
        print(f"❌ Error: {e}")


def main():
    # 1. Binance U-Margin
    try:
        ex1 = get_binance_sandbox("future")
        test_leverage(ex1, "BTC/USDT:USDT", "Binance U-Margin")
    except Exception as e:
        print(f"Binance Init Error: {e}")

    # 2. Binance Coin-Margin
    try:
        ex2 = get_binance_sandbox("delivery")
        test_leverage(ex2, "BTC/USD:BTC", "Binance Coin-Margin")
    except Exception as e:
        print(f"Binance Delivery Init Error: {e}")

    # 3. Kraken U-Margin
    try:
        ex3 = get_kraken_sandbox("future")
        test_leverage(ex3, "BTC/USD:USD", "Kraken U-Margin")
    except Exception as e:
        print(f"Kraken Future Init Error: {e}")

    # 4. Kraken Coin-Margin
    # NOTE: Kraken Futures handles both Linear and Inverse.
    # 'delivery' in utils.py maps to ccxt.kraken (Spot) which fails in sandbox.
    # We use 'future' (krakenfutures) for both.
    try:
        ex4 = get_kraken_sandbox("future")
        test_leverage(ex4, "BTC/USD:BTC", "Kraken Coin-Margin")
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
