from pathlib import Path
from typing import Any

import ccxt
import pytest

from src.cache_tool import DuckDbOhlcvCache
from src.domain_errors import ResponseRowLimitExceeded
from src.tools.ccxt_client import CcxtClient

MINUTE = 60_000


def _row(timestamp: int) -> list[float]:
    value = float(timestamp)
    return [timestamp, value, value + 2, value - 2, value + 1, 10]


def _minutes(*values: int) -> list[int]:
    return [value * MINUTE for value in values]


class FakeExchange:
    def __init__(self, times: list[int]) -> None:
        self.rows = [_row(timestamp) for timestamp in times]
        self.timeframes = {"1m": "1m", "1M": "1M"}
        self.linear = True
        self.ohlcv_calls: list[dict[str, Any]] = []
        self.create_calls = 0
        self.create_arguments: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.invalid_symbols: set[str] = set()
        self.positions: list[dict[str, Any]] = []
        self.closed = 0
        self.has = {
            "fetchOHLCV": True,
            "fetchBalance": True,
            "fetchTickers": True,
            "fetchPositions": True,
            "fetchOpenOrders": True,
            "fetchClosedOrders": True,
            "fetchOrder": True,
            "cancelOrder": True,
            "cancelAllOrders": True,
            "createOrder": True,
            "fetchMyTrades": True,
            "setLeverage": True,
            "setMarginMode": True,
        }

    def fetch_ohlcv(
        self, symbol, timeframe, since=None, limit: int = 1000, params=None
    ):
        self.ohlcv_calls.append(
            {"since": since, "limit": limit, "params": params or {}}
        )
        rows = self.rows
        if since is not None:
            return [row for row in rows if row[0] >= since][:limit]
        until = (params or {}).get("until")
        if until is not None:
            rows = [row for row in rows if row[0] <= until]
        return rows[-limit:]

    def fetch_open_orders(self, symbol, since, limit, params):
        suffix = "stop" if params.get("stop") else "normal"
        return [{"id": suffix, "timestamp": 2 if suffix == "stop" else 1}]

    def fetch_closed_orders(self, symbol, since, limit, params):
        return self.fetch_open_orders(symbol, since, limit, params)

    def fetch_order(self, order_id, symbol, params):
        if not params.get("stop"):
            raise ccxt.OrderNotFound("not found")
        return {"id": order_id, "status": "open"}

    def cancel_order(self, order_id, symbol, params):
        if not params.get("stop"):
            raise ccxt.OrderNotFound("not found")
        return {"id": order_id, "status": "canceled"}

    def cancel_all_orders(self, symbol, params):
        return [{"stop": bool(params.get("stop"))}]

    def create_order(self, *args, **kwargs):
        self.create_calls += 1
        self.create_arguments.append((args, kwargs))
        return {"id": "created"}

    def fetch_positions(self, symbols, params):
        return self.positions

    def fetch_balance(self, params):
        return {"total": {"USDT": 1}}

    def fetch_tickers(self, symbols, params):
        return {}

    def fetch_my_trades(self, symbol, since, limit, params):
        return []

    def set_leverage(self, leverage, symbol, params):
        return {"leverage": leverage}

    def set_margin_mode(self, mode, symbol, params):
        return {"marginMode": mode}

    def market(self, symbol):
        if symbol in self.invalid_symbols:
            raise ccxt.BadSymbol(symbol)
        return {
            "limits": {"amount": {"min": 0.001}},
            "precision": {"amount": 0.001},
            "linear": self.linear,
            "settle": "USDT",
            "contractSize": 1,
        }

    def close(self):
        self.closed += 1


def _cache(path: Path) -> DuckDbOhlcvCache:
    return DuckDbOhlcvCache(path / "cache.duckdb", 100_001, 200_000)


def _client(temp_dir, times=None, provider="binance", market="future"):
    exchange = FakeExchange(times or _minutes(1, 2, 3, 4, 5))
    return CcxtClient(exchange, provider, market, "sandbox", _cache(temp_dir)), exchange


def test_full_cache_hit_does_not_call_provider_again(temp_dir):
    client, exchange = _client(temp_dir)
    first = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)
    calls_after_first = len(exchange.ohlcv_calls)

    second = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)

    assert [row[0] for row in first.rows] == _minutes(1, 2)
    assert second.last_bar_completion_confirmed is True
    assert len(exchange.ohlcv_calls) == calls_after_first


def test_partial_cache_hit_fetches_from_cached_tail_with_overlap(temp_dir):
    client, exchange = _client(temp_dir)
    client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)
    exchange.ohlcv_calls.clear()

    result = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 3)

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3)
    assert exchange.ohlcv_calls[0]["since"] == 2 * MINUTE


def test_overlap_failure_discards_prefix_and_refetches_original_query(temp_dir):
    client, exchange = _client(temp_dir)
    client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)
    original_fetch = exchange.fetch_ohlcv
    omit_overlap = True

    def fetch_without_first_overlap(*args, **kwargs):
        nonlocal omit_overlap
        rows = original_fetch(*args, **kwargs)
        if omit_overlap and kwargs.get("since") == 2 * MINUTE:
            omit_overlap = False
            return [row for row in rows if row[0] != 2 * MINUTE]
        return rows

    exchange.fetch_ohlcv = fetch_without_first_overlap
    exchange.ohlcv_calls.clear()

    result = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 3)

    assert [call["since"] for call in exchange.ohlcv_calls[:2]] == [
        2 * MINUTE,
        MINUTE,
    ]
    assert [row[0] for row in result.rows] == _minutes(1, 2, 3)


def test_unconfirmed_tail_stays_in_response_but_not_cache(temp_dir):
    client, _ = _client(temp_dir, times=_minutes(1, 2, 3))

    response = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 3)
    cached = client.cache.read_best_prefix(
        client._series("BTC/USDT", "1m", "default").key, MINUTE, None
    )

    assert [row[0] for row in response.rows] == _minutes(1, 2, 3)
    assert response.last_bar_completion_confirmed is False
    assert [row[0] for row in cached] == _minutes(1, 2)


def test_enable_cache_false_disables_both_read_and_write(temp_dir):
    client, exchange = _client(temp_dir)

    client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2, enable_cache=False)
    client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2, enable_cache=False)

    assert len(exchange.ohlcv_calls) == 2
    count = (
        client.cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments")
        .fetchone()[0]
    )
    assert count == 0


def test_latest_limit_never_reads_cache_and_does_not_cache_tail(temp_dir):
    client, exchange = _client(temp_dir)

    result = client.fetch_ohlcv_latest_limit("BTC/USDT", "1m", 3)
    series = client._series("BTC/USDT", "1m", "default")
    cached = client.cache.read_best_prefix(series.key, 3 * MINUTE, None)

    assert [row[0] for row in result.rows] == _minutes(3, 4, 5)
    assert [row[0] for row in cached] == _minutes(3, 4)
    assert exchange.ohlcv_calls[0]["since"] is None


def test_since_latest_enforces_budget_across_cache_prefix(temp_dir, monkeypatch):
    client, _ = _client(temp_dir)
    client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 4)
    monkeypatch.setattr("src.tools.ccxt_client.MAX_RESPONSE_ROWS", 3)

    with pytest.raises(ResponseRowLimitExceeded):
        client.fetch_ohlcv_since_latest("BTC/USDT", "1m", MINUTE)


def test_kraken_spot_never_reads_or_writes_outer_cache(temp_dir):
    client, exchange = _client(temp_dir, provider="kraken", market="spot")

    client.fetch_ohlcv_since_limit("BTC/USD", "1m", MINUTE, 2)
    client.fetch_ohlcv_since_limit("BTC/USD", "1m", MINUTE, 2)

    assert len(exchange.ohlcv_calls) == 2
    count = (
        client.cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments")
        .fetchone()[0]
    )
    assert count == 0
