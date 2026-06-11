import argparse
import json
from typing import Any

from src.tools.tq_manager import tq_manager
from src.types_tq import TqOhlcvRequest, TqTickRequest, TqUnderlyingSymbolRequest


def _print_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(description="TQ route probe")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ohlcv = subparsers.add_parser("ohlcv")
    ohlcv.add_argument("--symbol", required=True)
    ohlcv.add_argument("--duration-seconds", type=int, required=True)
    ohlcv.add_argument("--data-length", type=int, default=10000)
    ohlcv.add_argument("--adj-type", default=None)

    tick = subparsers.add_parser("tick")
    tick.add_argument("--symbol", required=True)
    tick.add_argument("--data-length", type=int, default=10000)
    tick.add_argument("--adj-type", default=None)

    underlying = subparsers.add_parser("underlying")
    underlying.add_argument("--symbol", required=True)
    underlying.add_argument("--n", default=None)

    args = parser.parse_args()

    if args.command == "ohlcv":
        result = tq_manager.fetch_ohlcv(
            TqOhlcvRequest(
                symbol=args.symbol,
                duration_seconds=args.duration_seconds,
                data_length=args.data_length,
                adj_type=args.adj_type,
            )
        )
        _print_json(result)
        return

    if args.command == "tick":
        result = tq_manager.fetch_tick(
            TqTickRequest(
                symbol=args.symbol,
                data_length=args.data_length,
                adj_type=args.adj_type,
            )
        )
        _print_json(result)
        return

    n = int(args.n) if args.n not in (None, "") else None
    result = tq_manager.fetch_underlying_symbol(
        TqUnderlyingSymbolRequest(symbol=args.symbol, n=n)
    )
    _print_json(result.model_dump())


if __name__ == "__main__":
    main()
