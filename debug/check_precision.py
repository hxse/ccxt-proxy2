import sys
import os

# Allow importing from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_binance_sandbox, print_json


def main():
    try:
        exchange = get_binance_sandbox("future")
        symbol = "BTC/USDT"

        print(f"Fetching market info for {symbol}...")
        market = exchange.market(symbol)

        precision_amount = market["precision"]["amount"]
        limits_amount_min = market["limits"]["amount"]["min"]

        print(f"\nSymbol: {symbol}")
        print(f"Precision Mode: {exchange.precisionMode}")

        print(f"\nPrecision Amount:")
        print(f"  Value: {precision_amount}")
        print(f"  Type: {type(precision_amount)}")

        print(f"\nLimits Amount Min:")
        print(f"  Value: {limits_amount_min}")
        print(f"  Type: {type(limits_amount_min)}")

        # Check if precision is float or int-like
        if isinstance(precision_amount, int):
            print(f"  Interpretation: Decimal places (Integer)")
        else:
            print(f"  Interpretation: Step size (Float)")

        # Let's check a few other symbols to see if it varies
        for sym in ["ETH/USDT"]:
            if sym in exchange.markets:
                m = exchange.market(sym)
                print(f"\nSymbol: {sym}")
                print(
                    f"Precision Amount: {m['precision']['amount']} (Type: {type(m['precision']['amount'])})"
                )

    except Exception as e:
        print(f"Error: {e}")
    finally:
        pass


if __name__ == "__main__":
    main()
