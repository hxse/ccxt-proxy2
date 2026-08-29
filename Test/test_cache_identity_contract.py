import json

import duckdb
import pytest

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult, OhlcvSeries


def _row(timestamp: int):
    value = float(timestamp)
    return (timestamp, value, value + 2, value - 2, value + 1, 1.0)


def _cache(path) -> DuckDbOhlcvCache:
    return DuckDbOhlcvCache(path / "identity.duckdb", 100_001, 200_000)


def test_series_key_contains_every_data_identity_dimension():
    base = {
        "provider": "binance",
        "mode": "sandbox",
        "market": "future",
        "symbol": "BTC/USDT:USDT",
        "timeframe": "1m",
        "variant": "default",
    }
    series = [OhlcvSeries(**base)]
    replacements = {
        "provider": "kraken",
        "mode": "live",
        "market": "spot",
        "symbol": "ETH/USDT:USDT",
        "timeframe": "5m",
        "variant": "mark",
    }
    series.extend(
        OhlcvSeries(**(base | {field: value})) for field, value in replacements.items()
    )

    keys = {item.key for item in series}
    assert len(keys) == 7
    assert json.loads(series[0].key) == base


def test_cache_never_crosses_series_with_identical_timestamps(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series-a", OhlcvResult([_row(10), _row(20)], True), 10)

    assert cache.read_best_prefix("series-a", 10, None)
    assert cache.read_best_prefix("series-b", 10, None) == []


def test_deleted_segment_ids_are_not_reused(temp_dir):
    cache = _cache(temp_dir)
    cache.write_segment("series", OhlcvResult([_row(10)], True), 10)
    connection = cache._connection()
    first_id = connection.execute("SELECT segment_id FROM cache_segments").fetchone()[0]
    connection.execute("DELETE FROM ohlcv_rows")
    connection.execute("DELETE FROM cache_segments")

    cache.write_segment("series", OhlcvResult([_row(20)], True), 20)

    second_id = connection.execute("SELECT segment_id FROM cache_segments").fetchone()[
        0
    ]
    assert second_id > first_id


def test_unknown_duckdb_schema_version_is_rejected(temp_dir):
    path = temp_dir / "future-schema.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute(
        "CREATE TABLE cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
    )
    connection.execute("INSERT INTO cache_meta VALUES ('schema_version', 'future')")
    connection.close()

    with pytest.raises(RuntimeError, match="unsupported cache schema version: future"):
        DuckDbOhlcvCache(path, 100_001, 200_000)
