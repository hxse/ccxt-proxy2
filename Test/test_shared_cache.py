import json
import threading
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pytest

from src.cache_tool import (
    DuckDbOhlcvCache,
    OhlcvResult,
    TqOhlcvBatch,
    TqOhlcvSeries,
)
from src.cache_tool.duckdb_schema import ensure_schema
from src.tools.cache_resource import CacheResource
from src.tools.config_types import AppConfig, OhlcvCacheConfig
from src.tools.service_runtime import ServiceRuntime


@pytest.fixture
def cache(tmp_path):
    result = DuckDbOhlcvCache(tmp_path / "shared.duckdb", 100_001, 200_000)
    yield result
    result.close()


def ccxt_rows(*times, confirmed=True):
    return OhlcvResult([(t, 2.0, 4.0, 1.0, 3.0, 10.0) for t in times], confirmed)


def tq_batch(*times, confirmed=False, first_id=0):
    return TqOhlcvBatch(
        [
            {
                "datetime": t,
                "id": first_id + i,
                "open": 2.0,
                "high": 4.0,
                "low": 1.0,
                "close": 3.0,
                "volume": 10.0,
                "open_oi": None,
                "close_oi": 50.0,
            }
            for i, t in enumerate(times)
        ],
        confirmed,
    )


def test_backward_read_and_summary_do_not_join_disjoint_segments(cache):
    cache.write_segment("series", ccxt_rows(10, 20, 30), 1)
    cache.write_segment("series", ccxt_rows(80, 90), 80)
    assert [r[0] for r in cache.read_contiguous_before("series", 100, 20).rows] == [
        80,
        90,
    ]
    assert [r[0] for r in cache.read_contiguous_before("series", 25, 20).rows] == [
        10,
        20,
    ]
    assert cache.read_contiguous_before("series", 5, 20).rows == []
    summary = cache.read_latest_summary("series")
    assert (
        summary.start,
        summary.end,
        summary.count,
        summary.total_count,
        summary.segment_count,
    ) == (80, 90, 2, 5, 2)
    assert summary.time_unit == "ms"
    empty = cache.read_latest_summary("unknown")
    assert (empty.start, empty.end, empty.count, empty.total_count) == (
        None,
        None,
        0,
        0,
    )


def test_unknown_tail_cannot_connect_and_connected_read_clips_newer_data(cache):
    cache.write_segment("series", ccxt_rows(10, 20, 30, 40, 50), 10)
    assert (
        cache.read_connected_history(
            "series", ccxt_rows(5, 10, confirmed=False), 10
        ).rows
        == []
    )
    connected = cache.read_connected_history(
        "series", ccxt_rows(20, 30, confirmed=False), 10
    )
    assert [r[0] for r in connected.rows] == [10, 20, 30]


def test_tq_fields_tail_revision_and_bridge_use_same_segment_engine(cache):
    series = TqOhlcvSeries("KQ.m@SHFE.rb", 300, "FORWARD")
    assert series.key == TqOhlcvSeries(series.symbol, 300, "F").key
    assert json.loads(series.key)["timeframe"] == "300s"
    cache.write_tq_segment(series, tq_batch(10, 20, 30))
    cache.write_tq_segment(series, tq_batch(40, 50, 60, first_id=3))
    assert cache.read_latest_summary(series.key).segment_count == 2
    bridge = tq_batch(20, 30, 40, 50, first_id=1)
    bridge.records[0]["close_oi"] = 99.0
    cache.write_tq_segment(series, bridge)
    summary = cache.read_latest_summary(series.key)
    assert (summary.count, summary.segment_count, summary.time_unit) == (5, 1, "ns")
    rows = cache.read_contiguous_before(series.key, 100, 10).rows
    assert [r["datetime"] for r in rows] == [10, 20, 30, 40, 50]
    assert rows[1]["close_oi"] == 99.0
    assert all(r["symbol"] == series.symbol and r["duration"] == 300 for r in rows)
    assert rows[0]["open_oi"] is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("open", None),
        ("id", -1),
        ("id", True),
        ("volume", -1),
        ("symbol", "wrong"),
        ("duration", 60),
    ],
)
def test_ineligible_tq_batch_does_not_partially_persist(cache, field, value):
    batch = tq_batch(10, 20, 30)
    batch.records[1][field] = value
    series = TqOhlcvSeries("SHFE.rb2610", 300)
    cache.write_tq_segment(series, batch)
    assert cache.read_latest_summary(series.key).total_count == 0


def test_unknown_network_tail_does_not_downgrade_confirmed_row(cache):
    series = TqOhlcvSeries("SHFE.rb2610", 60)
    cache.write_tq_segment(series, tq_batch(10, 20, confirmed=True))
    batch = tq_batch(10, 20)
    batch.records[-1]["close"] = 2.5
    cache.write_tq_segment(series, batch)
    assert cache.read_contiguous_before(series.key, 20, 2).rows[-1]["close"] == 3.0


def test_bulk_write_keeps_nanosecond_precision_and_nullable_oi(cache):
    series = TqOhlcvSeries("SHFE.rb2610", 60)
    start = 1_700_000_000_000_000_001
    times = [start + i * 60_000_000_000 for i in range(10000)]
    cache.write_tq_segment(series, tq_batch(*times))
    records = cache.read_contiguous_before(series.key, times[-1], 10000).rows
    assert [row["datetime"] for row in records] == times[:-1]
    assert [row["id"] for row in records] == list(range(9999))
    assert all(row["open_oi"] is None and row["close_oi"] == 50 for row in records)


def test_unit_conflict_is_rejected_atomically(cache):
    series = TqOhlcvSeries("SHFE.rb2610", 60)
    cache.write_segment(series.key, ccxt_rows(10, 20), 10)
    with pytest.raises(ValueError, match="kind/unit conflict"):
        cache.write_tq_segment(series, tq_batch(10, 20, 30))
    assert cache.read_latest_summary(series.key).time_unit == "ms"


def test_capacity_compares_exact_ms_ns_and_preserves_other_kinds(cache):
    connection = cache._connection()
    connection.execute(
        "INSERT INTO cache_segments VALUES (999,'calendar',1,1,1,1,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'calendar','date')"
    )
    cache.max_rows_total = 3
    cache.write_segment("ms", ccxt_rows(2_000_000_000_000, 4_000_000_000_000_000), None)
    series = TqOhlcvSeries("SHFE.rb2610", 60)
    cache.write_tq_segment(
        series,
        tq_batch(1_000_000_000_000_000_000, 3_000_000_000_000_000_000, confirmed=True),
    )
    assert [
        r[0] for r in cache.read_contiguous_before("ms", 5_000_000_000_000_000, 10).rows
    ] == [4_000_000_000_000_000]
    assert cache.read_latest_summary(series.key).count == 1
    assert connection.execute(
        "SELECT data_kind FROM cache_segments WHERE segment_id=999"
    ).fetchone() == ("calendar",)


def test_schema_one_migration_keeps_ids_coverage_and_sequence(tmp_path):
    path = tmp_path / "v1.duckdb"
    connection = duckdb.connect(str(path))
    connection.execute("CREATE TABLE cache_meta(key VARCHAR PRIMARY KEY,value VARCHAR)")
    connection.execute("INSERT INTO cache_meta VALUES ('schema_version','1')")
    connection.execute("CREATE SEQUENCE cache_segment_id_seq START 42")
    connection.execute(
        "CREATE TABLE cache_segments(segment_id BIGINT PRIMARY KEY, series_key VARCHAR,covered_from BIGINT,first_time BIGINT,last_time BIGINT,row_count BIGINT,created_at TIMESTAMP,updated_at TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE ohlcv_rows(segment_id BIGINT,time BIGINT,open DOUBLE,high DOUBLE,low DOUBLE,close DOUBLE,volume DOUBLE, PRIMARY KEY(segment_id,time))"
    )
    connection.execute(
        "INSERT INTO cache_segments VALUES (41,'old',5,10,10,1,now(),now())"
    )
    connection.execute("INSERT INTO ohlcv_rows VALUES (41,10,2,4,1,3,10)")
    connection.close()
    migrated = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        assert migrated.read_best_prefix("old", 5, 10) == ccxt_rows(10).rows
        assert migrated._connection().execute(
            "SELECT segment_id,covered_from,data_kind,time_unit FROM cache_segments"
        ).fetchone() == (41, 5, "ohlcv", "ms")
        migrated.write_segment("new", ccxt_rows(20), 20)
        assert migrated._connection().execute(
            "SELECT segment_id FROM cache_segments WHERE series_key='new'"
        ).fetchone() == (42,)
    finally:
        migrated.close()


def test_schema_upgrade_failure_rolls_back_structure_and_version(tmp_path):
    connection = duckdb.connect(str(tmp_path / "failed.duckdb"))
    connection.execute("CREATE TABLE cache_meta(key VARCHAR PRIMARY KEY,value VARCHAR)")
    connection.execute("INSERT INTO cache_meta VALUES ('schema_version','1')")
    connection.execute("CREATE TABLE ohlcv_rows (sdk_id BIGINT)")
    with pytest.raises(duckdb.Error):
        ensure_schema(connection)
    assert connection.execute("SELECT value FROM cache_meta").fetchone() == ("1",)
    assert (
        connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_name='cache_segments'"
        ).fetchall()
        == []
    )
    connection.close()


@pytest.mark.parametrize("limit", [0, -1, 100_001, True, 1.5])
def test_new_history_limits_are_checked(cache, limit):
    with pytest.raises(ValueError, match="max_rows"):
        cache.read_contiguous_before("series", 20, limit)


def test_owner_concurrent_first_use_and_close(tmp_path):
    owner = CacheResource(
        OhlcvCacheConfig(database_path=str(tmp_path / "owned.duckdb"))
    )
    with ThreadPoolExecutor(max_workers=4) as pool:
        caches = list(pool.map(lambda _: owner.get(), range(12)))
    assert all(cache is caches[0] for cache in caches)
    owner.close()
    owner.close()
    with pytest.raises(RuntimeError, match="resource is closed"):
        owner.get()
    with pytest.raises(RuntimeError, match="is closed"):
        caches[0].read_latest_summary("x")


@pytest.mark.parametrize(
    "method",
    ["read_contiguous_before", "read_connected_history", "read_latest_summary"],
)
def test_new_reads_use_one_snapshot_during_bridge(cache, monkeypatch, method):
    cache.write_segment("series", ccxt_rows(10, 20), 10)
    entered, commit = threading.Event(), threading.Event()
    enforce = cache._enforce_capacity

    def pause(connection, series_key):
        entered.set()
        assert commit.wait(3)
        enforce(connection, series_key)

    monkeypatch.setattr(cache, "_enforce_capacity", pause)
    args = {
        "read_contiguous_before": ("series", 40, 10),
        "read_connected_history": ("series", ccxt_rows(20, 30, 40), 10),
        "read_latest_summary": ("series",),
    }[method]
    with ThreadPoolExecutor(max_workers=1) as pool:
        write = pool.submit(cache.write_segment, "series", ccxt_rows(20, 30, 40), 20)
        assert entered.wait(3)
        try:
            before = getattr(cache, method)(*args)
            if method == "read_latest_summary":
                assert (before.end, before.count, before.total_count) == (20, 2, 2)
            else:
                assert [r[0] for r in before.rows] == [10, 20]
        finally:
            commit.set()
        write.result(timeout=3)
    assert cache.read_latest_summary("series").count == 4


def test_empty_whitelist_can_own_cache_without_starting_any_provider(tmp_path):
    config = AppConfig(
        SECRET="offline",
        ohlcv_cache=OhlcvCacheConfig(database_path=str(tmp_path / "app.duckdb")),
    )
    runtime = ServiceRuntime(config)
    runtime.start(None, None, None)
    first = runtime.cache.get()
    assert runtime.ready and runtime.initialized == []
    runtime.close()
    with pytest.raises(RuntimeError, match="closed"):
        first.read_latest_summary("x")
    runtime.start(None, None, None)
    assert runtime.cache.get() is not first
    runtime.close()
