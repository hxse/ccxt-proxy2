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
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    "mode": "sandbox",
                }
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
                "service_whitelist": [
                    {
                        "service": "ccxt",
                        "exchange": "kraken",
                        "market": "spot",
                        "mode": "sandbox",
                    }
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
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    "mode": "sandbox",
                }
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
                        "mode": "sandbox",
                    },
                    {
                        "service": "ccxt",
                        "exchange": "binance",
                        "market": "future",
                        "mode": "sandbox",
                    },
                ],
            }
        )
