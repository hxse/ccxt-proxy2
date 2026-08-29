import threading
import time
from concurrent.futures import ThreadPoolExecutor

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult
from src.tools.ccxt_client import CcxtClient


class ConcurrentExchange:
    def __init__(self) -> None:
        self.has = {"fetchBalance": True}
        self._state_lock = threading.Lock()
        self.active = 0
        self.max_active = 0

    def fetch_balance(self, params):
        with self._state_lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.02)
        with self._state_lock:
            self.active -= 1
        return {"total": {}}


class PagingExchange(ConcurrentExchange):
    def __init__(self) -> None:
        super().__init__()
        self.has["fetchOHLCV"] = True
        self.timeframes = {"1m": "1m"}
        self.rows = [_row(60_000 * value) for value in range(1, 7)]

    def fetch_ohlcv(
        self, symbol, timeframe, *, since=None, limit: int = 1000, params=None
    ):
        rows = self.rows
        if since is not None:
            return [row for row in rows if row[0] >= since][:limit]
        until = (params or {}).get("until")
        if until is not None:
            rows = [row for row in rows if row[0] <= until]
        return rows[-limit:]

    def market(self, symbol):
        return {"linear": True}


def _row(timestamp: int):
    value = float(timestamp)
    return (timestamp, value, value + 2, value - 2, value + 1, 1.0)


def test_ccxt_attempts_are_serialized_per_client():
    exchange = ConcurrentExchange()
    client = CcxtClient(exchange, "binance", "future", "sandbox", None)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: client.fetch_balance(), range(2)))

    assert len(results) == 2
    assert exchange.max_active == 1


def test_cache_objects_for_one_database_share_the_write_lock(temp_dir):
    path = temp_dir / "cache.duckdb"
    first = DuckDbOhlcvCache(path, 100_001, 200_000)
    second = DuckDbOhlcvCache(path, 100_001, 200_000)

    assert first._write_lock is second._write_lock


def test_duckdb_writers_use_separate_connections_and_one_write_lock(temp_dir):
    cache = DuckDbOhlcvCache(temp_dir / "cache.duckdb", 100_001, 200_000)

    def write(series: str) -> None:
        cache.write_segment(series, OhlcvResult([_row(1), _row(2)], True), 1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(write, ["a", "b"]))

    count = (
        cache._connection().execute("SELECT COUNT(*) FROM cache_segments").fetchone()[0]
    )
    assert count == 2


def test_other_ccxt_call_can_run_between_ohlcv_pages(monkeypatch):
    exchange = PagingExchange()
    client = CcxtClient(exchange, "binance", "future", "sandbox", None)
    client._ohlcv.page_limit = 3
    first_page_ready = threading.Event()
    continue_pagination = threading.Event()
    original_page = client._ohlcv._page
    page_count = 0

    def pause_after_first_page(*args, **kwargs):
        nonlocal page_count
        rows = original_page(*args, **kwargs)
        page_count += 1
        if page_count == 1:
            first_page_ready.set()
            assert continue_pagination.wait(2)
        return rows

    monkeypatch.setattr(client._ohlcv, "_page", pause_after_first_page)

    with ThreadPoolExecutor(max_workers=2) as executor:
        future = executor.submit(
            client.fetch_ohlcv_since_limit, "BTC/USDT:USDT", "1m", 60_000, 4
        )
        assert first_page_ready.wait(2)
        assert client.fetch_balance() == {"total": {}}
        continue_pagination.set()
        result = future.result(timeout=2)

    assert len(result.rows) == 4


def test_duckdb_reader_sees_precommit_or_postcommit_snapshot(temp_dir, monkeypatch):
    cache = DuckDbOhlcvCache(temp_dir / "snapshot.duckdb", 100_001, 200_000)
    cache.write_segment("series", OhlcvResult([_row(1), _row(2)], True), 1)
    write_entered = threading.Event()
    allow_commit = threading.Event()
    original_enforce = cache._enforce_capacity

    def pause_before_commit(connection, series_key):
        write_entered.set()
        assert allow_commit.wait(2)
        return original_enforce(connection, series_key)

    monkeypatch.setattr(cache, "_enforce_capacity", pause_before_commit)

    with ThreadPoolExecutor(max_workers=2) as executor:
        writer = executor.submit(
            cache.write_segment,
            "series",
            OhlcvResult([_row(2), _row(3)], True),
            2,
        )
        assert write_entered.wait(2)
        reader = executor.submit(cache.read_best_prefix, "series", 1, None)
        try:
            before_commit = reader.result(timeout=2)
        finally:
            allow_commit.set()
        writer.result(timeout=2)

    after_commit = cache.read_best_prefix("series", 1, None)
    assert [row[0] for row in before_commit] == [1, 2]
    assert [row[0] for row in after_commit] == [1, 2, 3]
