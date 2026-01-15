import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from debug.utils import get_binance_sandbox
from src.responses import TickersResponse, TickerInfo, BalanceResponse


def main():
    ex = get_binance_sandbox("future")
    symbol = "BTC/USDT:USDT"

    print("\n=== Verifying Tickers Response ===")
    try:
        raw_tickers = ex.fetch_tickers([symbol])
        # Validate with Pydantic
        # TickersResponse expects {'tickers': Dict[str, TickerInfo]}
        # But our API returns {'tickers': raw_tickers}
        # Let's simulate what the router return value would be validated against

        # Convert raw tickers to TickerInfo objects to see if they validate
        validated_tickers = {}
        for s, t in raw_tickers.items():
            # Filter out keys that might not be in TickerInfo if strict, but default is usually ignore extra
            # Actually TickerInfo allows extra? No, default is ignore.
            validated_tickers[s] = TickerInfo(**t)

        resp = TickersResponse(tickers=validated_tickers)
        print("✅ TickersResponse validation passed")
    except Exception as e:
        print(f"❌ TickersResponse validation failed: {e}")

    print("\n=== Verifying Balance Response ===")
    try:
        raw_balance = ex.fetch_balance()
        # BalanceResponse now has a nested 'balance' field

        # Simulate what the router returns:
        router_return = {"balance": raw_balance}

        resp = BalanceResponse(**router_return)
        print("✅ BalanceResponse validation passed")

        # Check access to nested fields
        # Note: raw_balance keys like 'USDT' are in resp.balance (which accepts extra fields)
        if hasattr(resp.balance, "USDT") or "USDT" in resp.balance.__dict__:
            # With extra='allow', attributes are set if they are valid identifiers
            if hasattr(resp.balance, "USDT"):
                print(f"   USDT Free (via attr): {resp.balance.USDT.get('free')}")  # type: ignore
            else:
                # fallback if not attribute accessible (e.g. if key is invalid identifier)
                print("   USDT in extra fields")

    except Exception as e:
        print(f"❌ BalanceResponse validation failed: {e}")


if __name__ == "__main__":
    main()
