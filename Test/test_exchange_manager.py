import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.tools.ccxt_client import CcxtClient
from src.tools.config_types import AppConfig
from src.tools.exchange_manager import ExchangeManager


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
    temp_dir, monkeypatch, cache_resource_factory
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
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    'is_live': False,
                }
            ],
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    resource = cache_resource_factory(config.ohlcv_cache)
    manager = ExchangeManager()

    manager.init_from_config(config, resource.get())
    first = manager.get_client("binance", "future", "sandbox")
    second = manager.get_client("binance", "future", "sandbox")

    assert isinstance(first, CcxtClient)
    assert first is second
    assert first.exchange is raw
    assert raw.loaded == 1


def test_exchange_manager_rejects_non_whitelisted_identity(
    temp_dir, cache_resource_factory
):
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
    resource = cache_resource_factory(config.ohlcv_cache)
    manager = ExchangeManager()
    manager.init_from_config(config, resource.get())

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
                "service_whitelist": [
                    {
                        "service": "ccxt",
                        "exchange": "kraken",
                        "market": "spot",
                        'is_live': False,
                    }
                ],
            }
        )


def test_exchange_manager_closes_clients_during_reinitialize_and_shutdown(
    temp_dir, monkeypatch, cache_resource_factory
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
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    'is_live': False,
                }
            ],
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    resource = cache_resource_factory(config.ohlcv_cache)
    manager = ExchangeManager()

    manager.init_from_config(config, resource.get())
    first = manager.get_client("binance", "future", "sandbox").exchange
    manager.init_from_config(config, resource.get())
    second = manager.get_client("binance", "future", "sandbox").exchange

    assert first.closed == 1
    assert second.closed == 0
    manager.close()
    assert second.closed == 1
    assert resource.get().read_latest_summary("still usable").total_count == 0


def test_config_rejects_duplicate_whitelist_identity():
    with pytest.raises(ValidationError, match="duplicate service_whitelist identity"):
        AppConfig.model_validate(
            {
                "SECRET": "secret",
                "binance": {
                    "test": {"api_key": "key", "secret": "secret"},
                },
                "service_whitelist": [
                    {
                        "service": "ccxt",
                        "exchange": "binance",
                        "market": "future",
                        'is_live': False,
                    },
                    {
                        "service": "ccxt",
                        "exchange": "binance",
                        "market": "future",
                        'is_live': False,
                    },
                ],
            }
        )


def test_loading_or_failed_mode_does_not_block_or_close_another_mode(
    tmp_path, monkeypatch, cache_resource_factory
):
    config = AppConfig.model_validate(
        {
            "SECRET": "offline",
            "ohlcv_cache": {"database_path": str(tmp_path / "cache.duckdb")},
            "binance": {
                name: {"api_key": "offline", "secret": "offline"}
                for name in ("test", "live")
            },
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    'is_live': (mode == "live"),
                }
                for mode in ("sandbox", "live")
            ],
        }
    )
    blocked, healthy = FakeExchange(), FakeExchange()
    entered, release = threading.Event(), threading.Event()

    def load_markets():
        entered.set()
        assert release.wait(3)
        raise RuntimeError("load failed")

    monkeypatch.setattr(blocked, "load_markets", load_markets)
    monkeypatch.setattr(
        "src.tools.exchange_manager.get_binance_exchange",
        lambda config, market, mode: blocked if mode == "sandbox" else healthy,
    )
    cache = cache_resource_factory(config.ohlcv_cache).get()
    manager = ExchangeManager()
    manager.close()
    manager.configure(config)
    entries = [item for item in config.service_whitelist if item.service == "ccxt"]
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(manager.initialize, config, entries[0], cache)
        try:
            assert entered.wait(2)
            manager.initialize(config, entries[1], cache)
            assert not pending.done()
            assert manager.get_client("binance", "future", "live").exchange is healthy
            with pytest.raises(HTTPException) as error:
                manager.get_client("binance", "future", "sandbox")
            assert error.value.detail == {
                "code": "SERVICE_NOT_READY",
                "service": "ccxt/binance/future/sandbox",
            }
            release.set()
            with pytest.raises(RuntimeError, match="load failed"):
                pending.result(timeout=2)
            assert blocked.closed == 1 and healthy.closed == 0
            assert manager.get_client("binance", "future", "live").exchange is healthy
        finally:
            release.set()
            pending.exception(timeout=3)
            manager.close()
