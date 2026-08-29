from pathlib import Path

import pytest

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult
from src.domain_errors import CacheCapacityExceeded


def _row(timestamp: int, value: float | None = None):
    price = float(timestamp if value is None else value)
    return (timestamp, price, price + 2, price - 2, price + 1, 10.0)


def _cache(path: Path) -> DuckDbOhlcvCache:
    return DuckDbOhlcvCache(path / "ohlcv.duckdb", 100_001, 200_000)


def _result(times: list[int], confirmed: bool = True) -> OhlcvResult:
    rows = [_row(timestamp) for timestamp in times]
    return OhlcvResult(rows, confirmed if rows else None)


def test_exact_bracketed_and_leading_gap_hits(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([20, 30, 40]), 10)

    assert [row[0] for row in cache.read_best_prefix("series", 20, None)] == [
        20,
        30,
        40,
    ]
    assert [row[0] for row in cache.read_best_prefix("series", 25, None)] == [30, 40]
    assert [row[0] for row in cache.read_best_prefix("series", 15, None)] == [
        20,
        30,
        40,
    ]
    assert cache.read_best_prefix("series", 9, None) == []
    assert cache.read_best_prefix("series", 41, None) == []


def test_read_selects_one_segment_with_the_longest_prefix(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([10, 20]), 0)
    cache.write_segment("series", _result([15, 25, 35]), 0)

    rows = cache.read_best_prefix("series", 12, None)

    assert [row[0] for row in rows] == [15, 25, 35]


def test_read_best_prefix_honors_max_rows(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([10, 20, 30]), 10)

    rows = cache.read_best_prefix("series", 10, 2)

    assert [row[0] for row in rows] == [10, 20]


def test_earlier_verified_request_expands_covered_from(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([20, 30]), 15)

    cache.write_segment("series", _result([20, 30]), 10)

    metadata = (
        cache._connection()
        .execute("SELECT covered_from, first_time FROM cache_segments")
        .fetchone()
    )
    assert metadata == (10, 20)


def test_exact_overlap_merges_every_touched_segment(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([10, 20]), 10)
    cache.write_segment("series", _result([30, 40]), 30)
    incoming = OhlcvResult([_row(20, 200), _row(25, 250), _row(30, 300)], True)

    cache.write_segment("series", incoming, 20)

    rows = cache.read_best_prefix("series", 10, None)
    assert [row[0] for row in rows] == [10, 20, 25, 30, 40]
    assert next(row for row in rows if row[0] == 20)[4] == 201
    segment_count = (
        cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments WHERE series_key='series'")
        .fetchone()[0]
    )
    assert segment_count == 1


def test_no_timestamp_overlap_keeps_segments_separate(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([10, 30]), 10)
    cache.write_segment("series", _result([20, 40]), 20)

    count = (
        cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments WHERE series_key='series'")
        .fetchone()[0]
    )

    assert count == 2


def test_unknown_tail_is_not_persisted(temp_dir):
    cache = _cache(temp_dir)

    cache.write_segment("series", _result([10, 20, 30], confirmed=False), 10)

    assert [row[0] for row in cache.read_best_prefix("series", 10, None)] == [10, 20]


def test_single_unknown_tail_does_not_create_empty_segment(temp_dir):
    cache = _cache(temp_dir)

    cache.write_segment("series", _result([10], confirmed=False), 10)

    count = (
        cache._connection().execute("SELECT COUNT(*) FROM cache_segments").fetchone()[0]
    )
    assert count == 0


def test_invalid_incoming_row_does_not_overwrite_existing(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", _result([10, 20]), 10)
    bad = (20, 20.0, float("nan"), 18.0, 21.0, 10.0)

    cache.write_segment("series", OhlcvResult([bad], True), 20)

    rows = cache.read_best_prefix("series", 10, None)
    assert next(row for row in rows if row[0] == 20)[2] == 22.0


def test_one_invalid_row_rejects_the_whole_cache_batch(temp_dir):
    cache = _cache(temp_dir)
    bad = (20, 20.0, float("nan"), 18.0, 21.0, 10.0)

    cache.write_segment("series", OhlcvResult([_row(10), bad, _row(30)], True), 10)

    count = (
        cache._connection().execute("SELECT COUNT(*) FROM cache_segments").fetchone()[0]
    )
    assert count == 0


def test_series_capacity_evicts_old_prefix_to_watermark(temp_dir):
    cache = _cache(temp_dir)
    cache.max_rows_per_series = 10
    cache.max_rows_total = 100

    cache.write_segment("series", _result(list(range(12))), 0)

    assert cache.read_best_prefix("series", 0, None) == []
    assert [row[0] for row in cache.read_best_prefix("series", 3, None)] == list(
        range(3, 12)
    )
    metadata = (
        cache._connection()
        .execute("SELECT covered_from, first_time, row_count FROM cache_segments")
        .fetchone()
    )
    assert metadata == (3, 3, 9)


def test_global_capacity_counts_series_time_identities(temp_dir):
    cache = _cache(temp_dir)
    cache.max_rows_per_series = 100
    cache.max_rows_total = 10
    cache.write_segment("a", _result(list(range(8))), 0)
    cache.write_segment("b", _result(list(range(5))), 0)

    total = (
        cache._connection()
        .execute("""
        SELECT COUNT(*) FROM (SELECT s.series_key, r.time FROM ohlcv_rows r
        JOIN cache_segments s USING(segment_id) GROUP BY s.series_key, r.time)
    """)
        .fetchone()[0]
    )

    assert total == 9


def test_eviction_failure_rolls_back_the_whole_write(temp_dir, monkeypatch):
    cache = _cache(temp_dir)

    def fail(*args):
        raise RuntimeError("eviction failed")

    monkeypatch.setattr(cache, "_enforce_capacity", fail)

    with pytest.raises(CacheCapacityExceeded):
        cache.write_segment("series", _result([10, 20]), 10)
    count = (
        cache._connection().execute("SELECT COUNT(*) FROM cache_segments").fetchone()[0]
    )
    assert count == 0
