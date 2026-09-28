import argparse
import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from src.tools.cache_resource import CacheResource
from src.tools.shared import config
from src.tools.tq_manager import TqManager
from src.types_tq import TqOhlcvRequest, TqTickRequest, TqUnderlyingSymbolRequest

tq_manager = TqManager(config.tq, proxy_url=config.proxy.effective_http)


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
    underlying.add_argument("--start-time", type=int, default=None)
    underlying.add_argument("--end-time", type=int, default=None)
    underlying.add_argument("--transition-timeframe", default=None)
    underlying.add_argument("--transition-bars", type=int, default=10)

    args = parser.parse_args()
    if not any(item.service == "tq" for item in config.service_whitelist):
        parser.error("tq is not enabled in service_whitelist")
    with (
        TemporaryDirectory(prefix="tq-probe-") as directory,
        asyncio.Runner() as runner,
    ):
        resource = CacheResource(
            config.ohlcv_cache.model_copy(
                update={"database_path": str(Path(directory) / "probe.duckdb")}
            )
        )
        try:
            tq_manager.initialize(resource.get())
            runner.run(_query(args))
        finally:
            runner.run(tq_manager.close_metadata())
            tq_manager.close()
            resource.close()


async def _query(args):

    if args.command == "ohlcv":
        result = await tq_manager.fetch_ohlcv(
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

    try:
        result = await tq_manager.fetch_underlying_symbol(
            TqUnderlyingSymbolRequest(
                symbol=args.symbol,
                start_time=args.start_time,
                end_time=args.end_time,
                transition_timeframe=args.transition_timeframe,
                transition_bars=args.transition_bars,
            )
        )
        _print_json(result.model_dump())
    finally:
        await tq_manager.close_metadata()


if __name__ == "__main__":
    main()
