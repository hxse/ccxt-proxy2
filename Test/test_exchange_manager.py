import asyncio

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from src.tools.ccxt_client import CcxtClient
from src.tools.config_types import AppConfig
from src.tools.exchange_manager import ExchangeManager
from src.tools.shared import lifespan
from src.tools.telegram_manager import telegram_manager
from src.tools.tq_manager import tq_manager


class FakeExchange:
    def __init__(self) -> None:
        self.has = {}
        self.loaded = 0
        self.closed = 0

    def load_markets(self):
        self.loaded += 1
        return {}

    def close(self):
        self.closed += 1


def test_exchange_manager_returns_long_lived_client_not_raw_exchange(
    temp_dir, monkeypatch
):
    raw = FakeExchange()
    monkeypatch.setattr(
        "src.tools.exchange_manager.get_binance_exchange", lambda *args: raw
    )
    config = AppConfig.model_validate(
        {
            "SECRET": "secret",
            "binance": {
                "test": {"api_key": "key", "secret": "secret"},
            },
            "exchange_whitelist": [
                {"exchange": "binance", "market": "future", "mode": "sandbox"}
            ],
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    manager = ExchangeManager()

    manager.init_from_config(config)
    first = manager.get_client("binance", "future", "sandbox")
    second = manager.get_client("binance", "future", "sandbox")

    assert isinstance(first, CcxtClient)
    assert first is second
    assert first.exchange is raw
    assert raw.loaded == 1


def test_exchange_manager_rejects_non_whitelisted_identity(temp_dir):
    config = AppConfig.model_validate(
        {
            "SECRET": "secret",
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    manager = ExchangeManager()
    manager.init_from_config(config)

    with pytest.raises(HTTPException) as exc_info:
        manager.get_client("kraken", "spot", "live")
    assert exc_info.value.status_code == 503


def test_config_rejects_kraken_spot_sandbox():
    with pytest.raises(ValidationError, match="kraken spot sandbox is not supported"):
        AppConfig.model_validate(
            {
                "SECRET": "secret",
                "kraken": {
                    "test": {"api_key": "key", "secret": "secret"},
                },
                "exchange_whitelist": [
                    {"exchange": "kraken", "market": "spot", "mode": "sandbox"}
                ],
            }
        )


def test_exchange_manager_closes_clients_during_reinitialize_and_shutdown(
    temp_dir, monkeypatch
):
    exchanges = [FakeExchange(), FakeExchange()]
    monkeypatch.setattr(
        "src.tools.exchange_manager.get_binance_exchange",
        lambda *args: exchanges.pop(0),
    )
    config = AppConfig.model_validate(
        {
            "SECRET": "secret",
            "binance": {"test": {"api_key": "key", "secret": "secret"}},
            "exchange_whitelist": [
                {"exchange": "binance", "market": "future", "mode": "sandbox"}
            ],
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    manager = ExchangeManager()

    manager.init_from_config(config)
    first = manager.get_client("binance", "future", "sandbox").exchange
    manager.init_from_config(config)
    second = manager.get_client("binance", "future", "sandbox").exchange

    assert first.closed == 1
    assert second.closed == 0
    manager.close()
    assert second.closed == 1


def test_config_rejects_duplicate_whitelist_identity():
    with pytest.raises(ValidationError, match="duplicate exchange_whitelist identity"):
        AppConfig.model_validate(
            {
                "SECRET": "secret",
                "binance": {
                    "test": {"api_key": "key", "secret": "secret"},
                },
                "exchange_whitelist": [
                    {"exchange": "binance", "market": "future", "mode": "sandbox"},
                    {"exchange": "binance", "market": "future", "mode": "sandbox"},
                ],
            }
        )


def test_application_lifespan_closes_all_long_lived_resources(monkeypatch):
    events: list[str] = []
    manager = ExchangeManager()
    monkeypatch.setattr(manager, "init_from_config", lambda _: events.append("init"))
    monkeypatch.setattr(manager, "close", lambda: events.append("ccxt"))
    monkeypatch.setattr(telegram_manager, "close", lambda: events.append("telegram"))
    monkeypatch.setattr(tq_manager, "close", lambda: events.append("tq"))
    test_app = FastAPI()

    async def run_lifespan() -> None:
        async with lifespan(test_app):
            assert test_app.state.exchange_registry_ready is True

    asyncio.run(run_lifespan())

    assert events == ["init", "telegram", "tq", "ccxt"]
    assert test_app.state.exchange_registry_ready is False


def test_application_lifespan_fails_fast_when_registry_initialization_fails(
    monkeypatch,
):
    manager = ExchangeManager()

    def fail(_):
        raise RuntimeError("registry initialization failed")

    monkeypatch.setattr(manager, "init_from_config", fail)
    test_app = FastAPI()

    async def run_lifespan() -> None:
        async with lifespan(test_app):
            pytest.fail("failed initialization must not enter the service lifespan")

    with pytest.raises(RuntimeError, match="registry initialization failed"):
        asyncio.run(run_lifespan())

    assert test_app.state.exchange_registry_ready is False
    assert test_app.state.exchange_registry_initialized == []
    assert not hasattr(test_app.state, "exchange_registry_error")
