import sys
import os

# Allow importing from root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from debug.utils import get_binance_sandbox, print_json


def check_symbol(exchange, symbol):
    print(f"\n{'=' * 40}")
    print(f"Checking {symbol}")
    print(f"{'=' * 40}")

    try:
        market = exchange.market(symbol)

        # 1. Linear (bool)
        linear = market.get("linear")
        print(f"Linear: {linear} (Type: {type(linear)})")

        # 2. Settle (str)
        settle = market.get("settle")
        print(f"Settle: {settle} (Type: {type(settle)})")

        # 3. Precision Amount (float)
        prec_amount = market["precision"]["amount"]
        print(f"Precision Amount: {prec_amount} (Type: {type(prec_amount)})")

        # 4. Min Amount (float)
        min_amount = market["limits"]["amount"]["min"]
        print(f"Min Amount: {min_amount} (Type: {type(min_amount)})")

        # 5. Contract Size (float)
        contract_size = market.get("contractSize")
        print(f"Contract Size: {contract_size} (Type: {type(contract_size)})")

        # 6. Leverage (int) - This logic in ccxt_utils comes from fetch_positions
        # We need to simulate fetching positions or just check what fetch_positions returns safely
        # But here we can just check if we can fetch valid leverage info from market properties or account
        # The current implementation defaults to 1 if no position.
        # Let's see if we can get leverage tiers or max leverage from market info itself?
        # Actually ccxt_utils uses: current_leverage = int(pos.get("leverage", 1))
        # So we should check what type pos['leverage'] usually is.
        # But we might not have a position.
        # Let's try to fetch positions.
        try:
            positions = exchange.fetch_positions([symbol])
            print(f"Positions: {len(positions)}")
            if positions:
                lev = positions[0].get("leverage")
                print(f"Pos Leverage: {lev} (Type: {type(lev)})")
        except Exception as e:
            print(f"Fetch Positions Error: {e}")

    except Exception as e:
        print(f"Error checking {symbol}: {e}")


def main():
    try:
        exchange = get_binance_sandbox("future")

        # Check BTC/USDT (Linear)
        check_symbol(exchange, "BTC/USDT")

        # Check ETH/USDT (Linear)
        check_symbol(exchange, "ETH/USDT")

        # If possible, check a Coin-Margined symbol (Inverse)
        # Sandbox might not have them enabled or different symbols
        # BTC/USD:BTC or similar.
        # Let's list some markets to see available
        # print("Available markets sample:", list(exchange.markets.keys())[:5])

    except Exception as e:
        print(f"Global Error: {e}")


if __name__ == "__main__":
    main()
