import atexit
import json
from typing import Any

from fastapi import HTTPException

from src.base_types import ExchangeName, MarketType, ModeType
from src.tools.ccxt_client import CcxtClient
from src.tools.exchange_manager import exchange_manager
from src.tools.shared import config


atexit.register(exchange_manager.close)


def get_debug_client(
    exchange_name: ExchangeName,
    market: MarketType = "future",
    mode: ModeType = "sandbox",
) -> CcxtClient:
    """Return the production Client boundary for an enabled debug identity."""
    try:
        return exchange_manager.get_client(exchange_name, market, mode)
    except HTTPException as exc:
        if exc.status_code != 503:
            raise
        exchange_manager.init_from_config(config)
        return exchange_manager.get_client(exchange_name, market, mode)


def get_research_exchange(
    exchange_name: ExchangeName,
    market: MarketType = "future",
    mode: ModeType = "sandbox",
) -> Any:
    """Explicit raw Provider escape hatch for isolated research scripts only."""
    return get_debug_client(exchange_name, market, mode).exchange


def print_json(data: Any, title: str = "") -> None:
    if title:
        print(f"\n{'=' * 60}\n{title}\n{'=' * 60}")
    print(json.dumps(data, indent=2, default=str, ensure_ascii=False))
