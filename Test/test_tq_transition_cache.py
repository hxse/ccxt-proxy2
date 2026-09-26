import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pytest

from src.cache_tool import DuckDbOhlcvCache, TqOhlcvBatch
from src.cache_tool.transition_store import TransitionIdentity
from Test.test_tq_metadata_conversion import ns


@pytest.fixture
def cache(tmp_path):
    cache = DuckDbOhlcvCache(tmp_path / "window.duckdb", 100_001, 200_000)
    yield cache
    cache.close()


def identity():
    return TransitionIdentity(
        "KQ.m@SHFE.rb", date(2026, 9, 24), "SHFE.rb2610", "SHFE.rb2609", 300
    )


def batch(count, price=3.0):
    return TqOhlcvBatch(
        [
            {
                "datetime": ns("2026-09-24T09:00:00") + i * 300_000_000_000,
                "id": i,
                "open": 2.0,
                "high": 4.0,
                "low": 1.0,
                "close": price,
                "volume": 10.0,
            }
            for i in range(count + 1)
        ]
    )


def test_lengths_five_three_ten_and_equal_length_revision(cache):
    key = identity()
    cache.submit_transition_window(key, 5, batch(5))
    assert len(cache.read_transition_prefix(key, 3)) == 3
    assert cache.read_transition_prefix(key, 6) is None
    cache.submit_transition_window(key, 3, batch(3, 3.5))
    assert cache.read_transition_prefix(key, 5)[0]["old_close"] == 3.0
    cache.submit_transition_window(key, 10, batch(10, 3.2))
    assert len(cache.read_transition_prefix(key, 10)) == 10
    cache.submit_transition_window(key, 10, batch(10, 3.3))
    assert all(row["old_close"] == 3.3 for row in cache.read_transition_prefix(key, 10))
    assert cache._connection().execute(
        "SELECT COUNT(*) FROM ohlcv_rows"
    ).fetchone() == (0,)


@pytest.mark.parametrize("failure", ["short", "gap", "proof_nan", "confirmed"])
def test_invalid_whole_window_never_replaces_existing(cache, failure):
    key = identity()
    cache.submit_transition_window(key, 3, batch(3))
    incoming = batch(5)
    if failure == "short":
        incoming.records.pop()
    elif failure == "gap":
        incoming.records[-1]["id"] += 1
    elif failure == "proof_nan":
        incoming.records[-1]["volume"] = float("nan")
    else:
        incoming = TqOhlcvBatch(incoming.records, True)
    with pytest.raises(ValueError):
        cache.submit_transition_window(key, 5, incoming)
    assert cache.read_transition_prefix(key, 4) is None
    assert len(cache.read_transition_prefix(key, 3)) == 3


def test_window_write_failure_rolls_back_rows_and_metadata(cache, monkeypatch):
    from src.cache_tool import transition_store

    key = identity()
    cache.submit_transition_window(key, 3, batch(3))
    original = transition_store.submit

    def fail(*args):
        original(*args)
        raise RuntimeError("offline rollback")

    monkeypatch.setattr(transition_store, "submit", fail)
    with pytest.raises(RuntimeError, match="offline rollback"):
        cache.submit_transition_window(key, 5, batch(5, 3.2))
    assert cache.read_transition_prefix(key, 4) is None
    assert cache.read_transition_prefix(key, 3)[0]["old_close"] == 3.0


def test_concurrent_reader_sees_whole_old_or_new_window(cache, monkeypatch):
    from src.cache_tool import transition_store

    key = identity()
    cache.submit_transition_window(key, 3, batch(3))
    entered, release = threading.Event(), threading.Event()
    original = transition_store.submit

    def delayed(*args):
        original(*args)
        entered.set()
        assert release.wait(3)

    monkeypatch.setattr(transition_store, "submit", delayed)
    with ThreadPoolExecutor(max_workers=1) as pool:
        write = pool.submit(cache.submit_transition_window, key, 5, batch(5, 3.5))
        assert entered.wait(3)
        try:
            assert cache.read_transition_prefix(key, 5) is None
            assert cache.read_transition_prefix(key, 3)[0]["old_close"] == 3.0
        finally:
            release.set()
        write.result(timeout=3)
    assert cache.read_transition_prefix(key, 5)[0]["old_close"] == 3.5
