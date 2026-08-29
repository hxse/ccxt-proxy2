import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from src.domain_errors import ProviderClientClosed
from src.tools.ccxt_client import CcxtClient
from src.tools.config_types import AppConfig
from src.tools.exchange_manager import ExchangeManager


class BlockingExchange:
    def __init__(self) -> None:
        self.has = {"fetchBalance": True}
        self.read_started = threading.Event()
        self.allow_read = threading.Event()
        self.closed = 0

    def fetch_balance(self, params):
        self.read_started.set()
        assert self.allow_read.wait(2)
        return {"total": {}}

    def close(self):
        self.closed += 1


def test_ccxt_close_waits_for_active_provider_call_and_rejects_new_calls():
    exchange = BlockingExchange()
    client = CcxtClient(exchange, "binance", "future", "sandbox", None)
    close_started = threading.Event()
    original_close = client.close

    def observed_close() -> None:
        close_started.set()
        original_close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        reader = executor.submit(client.fetch_balance)
        assert exchange.read_started.wait(2)
        closer = executor.submit(observed_close)
        assert close_started.wait(2)
        assert not closer.done()
        exchange.allow_read.set()
        assert reader.result(timeout=2) == {"total": {}}
        closer.result(timeout=2)

    assert exchange.closed == 1
    with pytest.raises(ProviderClientClosed):
        client.fetch_balance()


class InitializingExchange:
    def __init__(self, *, fail_load: bool = False) -> None:
        self.has = {}
        self.fail_load = fail_load
        self.closed = 0

    def load_markets(self):
        if self.fail_load:
            raise RuntimeError("load markets failed")
        return {}

    def close(self):
        self.closed += 1


def test_registry_initialization_failure_closes_every_partial_resource(
    temp_dir, monkeypatch
):
    binance = InitializingExchange()
    kraken = InitializingExchange(fail_load=True)
    monkeypatch.setattr(
        "src.tools.exchange_manager.get_binance_exchange",
        lambda *args: binance,
    )
    monkeypatch.setattr(
        "src.tools.exchange_manager.get_kraken_exchange",
        lambda *args: kraken,
    )
    config = AppConfig.model_validate(
        {
            "SECRET": "secret",
            "binance": {"test": {"api_key": "key", "secret": "secret"}},
            "kraken": {"test": {"api_key": "key", "secret": "secret"}},
            "exchange_whitelist": [
                {"exchange": "binance", "market": "future", "mode": "sandbox"},
                {"exchange": "kraken", "market": "future", "mode": "sandbox"},
            ],
            "ohlcv_cache": {
                "database_path": str(temp_dir / "cache.duckdb"),
                "max_rows_per_series": 100_001,
                "max_rows_total": 200_000,
            },
        }
    )
    manager = ExchangeManager()
    manager.close()

    with pytest.raises(RuntimeError, match="load markets failed"):
        manager.init_from_config(config)

    assert binance.closed == 1
    assert kraken.closed == 1
    with pytest.raises(HTTPException) as exc_info:
        manager.get_client("binance", "future", "sandbox")
    assert exc_info.value.status_code == 503
