"""覆盖曾实际生成的无 roll_date 的 schema 3/4，不能只验证新建库。"""

from dataclasses import replace
from datetime import date

import duckdb
import pytest

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult, OhlcvSeries, duckdb_schema
from src.cache_tool.maintenance_models import RetentionPolicy
from Test.test_tq_metadata_cache import calendar, mapping
from Test.test_tq_transition_cache import batch, identity

KEY = OhlcvSeries("binance", "live", "future", "BTC/USDT:USDT", "1m").key


def legacy_database(path, version="4", metadata=True):
    cache = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        cache.write_segment(
            KEY, OhlcvResult([(i, 1.0, 2.0, 0.0, 1.0, 5.0) for i in (10, 20)], True), 1
        )
        if metadata:
            cache.submit_calendar(calendar(1, 3))
            cache.submit_mapping(mapping())
            if version == "4":
                cache.submit_transition_window(identity(), 3, batch(3))
    finally:
        cache.close()
    with duckdb.connect(str(path)) as db:
        # 固定为旧版真实表结构：日行无节点关联，节点主键只有片段和日期。
        db.execute("""CREATE TABLE legacy_rows(segment_id BIGINT,symbol VARCHAR NOT NULL,
            trading_date DATE,underlying_symbol VARCHAR NOT NULL,PRIMARY KEY(segment_id,trading_date))""")
        db.execute(
            "INSERT INTO legacy_rows SELECT segment_id,symbol,trading_date,underlying_symbol FROM mapping_rows"
        )
        db.execute("DROP TABLE mapping_rows")
        db.execute("ALTER TABLE legacy_rows RENAME TO mapping_rows")
        db.execute("""CREATE TABLE legacy_context(segment_id BIGINT,roll_date DATE,
            underlying_symbol VARCHAR NOT NULL,old_symbol VARCHAR,PRIMARY KEY(segment_id,roll_date))""")
        db.execute("INSERT INTO legacy_context SELECT * FROM mapping_context")
        db.execute("DROP TABLE mapping_context")
        db.execute("ALTER TABLE legacy_context RENAME TO mapping_context")
        if version == "3":
            db.execute("DROP TABLE transition_windows")
            db.execute("DROP TABLE transition_rows")
        db.execute(
            "UPDATE cache_meta SET value=? WHERE key='schema_version'", [version]
        )
        return db.execute(
            "SELECT segment_id,series_key,covered_from,first_time,last_time,row_count FROM cache_segments ORDER BY segment_id"
        ).fetchall()


@pytest.mark.parametrize("version", ["3", "4"])
@pytest.mark.parametrize("metadata", [False, True])
def test_legacy_upgrade_preserves_data_and_refetches_unproven_node_links(
    tmp_path, version, metadata
):
    path = tmp_path / "legacy.duckdb"
    segments = legacy_database(path, version, metadata)
    cache = DuckDbOhlcvCache(path, 100_001, 200_000)
    mapped = mapping()
    try:
        assert cache._connection().execute(
            "SELECT value FROM cache_meta WHERE key='schema_version'"
        ).fetchone() == ("5",)
        assert (
            cache._connection()
            .execute(
                "SELECT segment_id,series_key,covered_from,first_time,last_time,row_count FROM cache_segments ORDER BY segment_id"
            )
            .fetchall()
            == segments
        )
        assert [row[0] for row in cache.read_best_prefix(KEY, 1, 10)] == [10, 20]
        dates = [row.date for row in mapped.records]
        assert (
            cache.read_mapping_range(mapped.symbol, dates, mapped.facts.verified_date)
            is None
        )
        if metadata:
            assert (
                cache.read_calendar_range(date(2026, 9, 1), date(2026, 9, 3))
                is not None
            )
            assert cache._connection().execute(
                "SELECT COUNT(*) FROM mapping_rows WHERE roll_date IS NULL"
            ).fetchone() == (4,)
            assert (
                cache.read_metadata_facts("main_mapping", mapped.symbol) == mapped.facts
            )
            if version == "4":
                assert len(cache.read_transition_prefix(identity(), 3)) == 3
        cache.submit_mapping(mapped)
        result = cache.read_mapping_range(
            mapped.symbol, dates, mapped.facts.verified_date
        )
        assert result is not None and result.nodes == mapped.nodes
        changed = replace(mapped.nodes[0], underlying_symbol="SHFE.rb2609")
        revised = replace(
            mapped,
            records=[
                replace(mapped.records[0], underlying_symbol=changed.underlying_symbol)
            ],
            nodes=[changed],
        )
        cache.submit_mapping(revised)
        assert cache._connection().execute(
            "SELECT COUNT(DISTINCT underlying_symbol) FROM mapping_context WHERE roll_date=?",
            [changed.date],
        ).fetchone() == (2,)
    finally:
        cache.close()
    reopened = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        assert reopened.read_latest_summary(KEY).count == 2
        result = reopened.read_mapping_range(
            mapped.symbol, [mapped.records[0].date], mapped.facts.verified_date
        )
        assert result is not None and result.nodes == [changed]
    finally:
        reopened.close()


def test_legacy_upgrade_failure_rolls_back_columns_context_and_version(
    tmp_path, monkeypatch
):
    path = tmp_path / "rollback.duckdb"
    legacy_database(path)
    upgrade = duckdb_schema.upgrade_mapping_links

    def fail(connection):
        upgrade(connection)
        raise RuntimeError("offline migration failure")

    monkeypatch.setattr(duckdb_schema, "upgrade_mapping_links", fail)
    with duckdb.connect(str(path)) as db:
        with pytest.raises(RuntimeError, match="offline migration failure"):
            duckdb_schema.ensure_schema(db)
        assert db.execute(
            "SELECT value FROM cache_meta WHERE key='schema_version'"
        ).fetchone() == ("4",)
        assert "roll_date" not in {
            row[1] for row in db.execute("PRAGMA table_info('mapping_rows')").fetchall()
        }
        assert db.execute("SELECT COUNT(*) FROM mapping_rows").fetchone() == (4,)
        assert db.execute("SELECT COUNT(*) FROM mapping_context").fetchone() == (2,)


def test_retention_keeps_legacy_context_until_reliable_refetch(tmp_path):
    path = tmp_path / "retention.duckdb"
    legacy_database(path)
    cache = DuckDbOhlcvCache(path, 100_001, 200_000)
    mapped = mapping()
    try:
        report = cache.prune(RetentionPolicy(("tq",), 30000, 0, 10), date(2026, 9, 3))
        assert report["status"] == "completed"
        assert cache._connection().execute(
            "SELECT COUNT(*) FROM mapping_context WHERE roll_date=?",
            [mapped.nodes[0].date],
        ).fetchone() == (1,)
        assert (
            cache.read_mapping_range(
                mapped.symbol, [date(2026, 9, 3)], mapped.facts.verified_date
            )
            is None
        )
        cache.submit_mapping(replace(mapped, records=mapped.records[-1:]))
        result = cache.read_mapping_range(
            mapped.symbol, [date(2026, 9, 3)], mapped.facts.verified_date
        )
        assert result is not None and result.nodes == mapped.nodes
    finally:
        cache.close()


def test_schema_four_with_valid_links_keeps_trusted_mapping(tmp_path):
    path = tmp_path / "linked.duckdb"
    mapped = mapping()
    cache = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        cache.submit_mapping(mapped)
    finally:
        cache.close()
    with duckdb.connect(str(path)) as db:
        db.execute("UPDATE cache_meta SET value='4' WHERE key='schema_version'")
    reopened = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        result = reopened.read_mapping_range(
            mapped.symbol,
            [row.date for row in mapped.records],
            mapped.facts.verified_date,
        )
        assert result is not None and result.nodes == mapped.nodes
    finally:
        reopened.close()
