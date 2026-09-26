import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import date

import pytest

from src.cache_tool import (
    DuckDbOhlcvCache,
    OhlcvResult,
    OhlcvSeries,
    TqOhlcvBatch,
    TqOhlcvSeries,
)
from src.cache_tool.maintenance_models import MaintenanceFailure, RetentionPolicy
from src.cache_tool.metadata_models import (
    CalendarDay,
    CalendarFacts,
    CalendarSourceResult,
    MappingDay,
)
from src.cache_tool.transition_store import TransitionIdentity
from src.tools.market_data_dates import years_before
from Test.test_shared_cache import tq_batch
from Test.test_tq_metadata_cache import mapping
from Test.test_tq_metadata_conversion import ns
from Test.test_tq_transition_cache import batch


@pytest.fixture
def cache(tmp_path):
    result = DuckDbOhlcvCache(tmp_path / "retention.duckdb", 100_001, 200_000)
    yield result
    result.close()


def series(mode="live", symbol="BTC/USDT", variant="default"):
    return OhlcvSeries("binance", mode, "future", symbol, "1m", variant)


def rows(*times):
    return OhlcvResult([(t, 1.0, 2.0, 0.0, 1.0, 2.0) for t in times], True)


def test_keep_newest_across_fragments_and_clear_sandbox_without_joining(cache):
    live, sandbox = series(), series("sandbox")
    cache.write_segment(live.key, rows(10, 20, 30, 40), 1)
    cache.write_segment(live.key, rows(90, 100), 90)
    cache.write_segment(sandbox.key, rows(10, 20), 10)
    result = cache.prune(RetentionPolicy(("ccxt",), 4, 0))
    assert result["status"] == "completed"
    assert result["ohlcv"] == {"series_processed": 2, "deleted_rows": 4}
    overview = cache.list_series_summaries({"provider": "binance"})
    assert len(overview) == 1
    assert (
        overview[0]["start"],
        overview[0]["end"],
        overview[0]["count"],
        overview[0]["total_count"],
        overview[0]["segment_count"],
    ) == (90, 100, 2, 4, 2)
    assert cache.read_best_prefix(live.key, 1, 10) == []
    assert [r[0] for r in cache.read_best_prefix(live.key, 30, 10)] == [30, 40]
    assert "K" not in overview[0] and "segment_id" not in overview[0]


def test_tq_nanoseconds_extra_fields_and_variants_are_isolated(cache):
    normal = TqOhlcvSeries("KQ.m@SHFE.rb", 300)
    adjusted = TqOhlcvSeries(normal.symbol, 300, "F")
    for item in (normal, adjusted):
        cache.write_tq_segment(
            item,
            tq_batch(
                1000000000000000001,
                1000000000000000002,
                1000000000000000003,
                confirmed=True,
            ),
        )
    cache.write_segment(series().key, rows(1000, 2000), 1000)
    cache.prune(RetentionPolicy(("tq",), 2, 0))
    for item in (normal, adjusted):
        cached = cache.read_contiguous_before(item.key, 2**63 - 1, 10).rows
        assert [row["datetime"] for row in cached] == [
            1000000000000000002,
            1000000000000000003,
        ]
        assert all(row["close_oi"] == 50.0 for row in cached)
    assert cache.read_latest_summary(series().key).count == 2
    assert (
        cache.list_series_summaries(
            {"provider": "tq", "timeframe": "300s", "variant": "F"}
        )[0]["count"]
        == 2
    )


def test_auxiliary_dates_keep_true_left_node_future_calendar_and_whole_crossing_window(
    cache,
):
    days = [date(2026, 9, day) for day in (1, 2, 3, 4)]
    cal = CalendarSourceResult(
        [CalendarDay(day, True) for day in days],
        CalendarFacts(
            date(2026, 10, 7),
            date(2010, 1, 1),
            date(2026, 12, 31),
            "holiday",
            1790000000000,
        ),
    )
    cache.submit_calendar(cal)
    mapped = mapping()
    cache.submit_mapping(mapped)
    identity = TransitionIdentity(
        "KQ.m@SHFE.hc", date(2026, 9, 1), "SHFE.hc2610", "SHFE.hc2605", 300
    )
    data = TqOhlcvBatch(
        [
            dict(row, datetime=ns(f"2026-09-0{i + 1}T09:00:00"))
            for i, row in enumerate(batch(3).records)
        ]
    )
    cache.submit_transition_window(identity, 3, data)
    facts = cache.read_metadata_facts("main_mapping", mapped.symbol)
    result = cache.prune(RetentionPolicy(("tq",), 0, 0, 10), date(2026, 9, 3))
    assert result["status"] == "completed"
    assert result["auxiliary"]["deleted_rows"] == 4
    assert cache.read_calendar_range(date(2026, 9, 3), date(2026, 9, 4)) is not None
    read = cache.read_mapping_range(
        mapped.symbol, [date(2026, 9, 3)], mapped.facts.verified_date
    )
    assert read is not None and read.nodes[0].date == date(2026, 8, 10)
    assert read.nodes[0].old_symbol == "SHFE.rb2605"
    assert len(cache.read_transition_prefix(identity, 3)) == 3
    assert cache.read_metadata_facts("main_mapping", mapped.symbol) == facts
    cache.prune(RetentionPolicy(("tq",), 0, 0, 10), date(2026, 9, 4))
    assert cache.read_transition_prefix(identity, 1) is None
    assert cache.list_series_summaries({"kind": "transition"}) == []


def test_partial_item_failure_rolls_back_one_identity_and_continues(cache, monkeypatch):
    from src.cache_tool import retention

    a, b = series(symbol="AAA"), series(symbol="BBB")
    cache.write_segment(a.key, rows(1, 2, 3), 1)
    cache.write_segment(b.key, rows(1, 2, 3), 1)
    original = retention._ordinary

    def fail_first(cache_, connection, key, limit):
        result = original(cache_, connection, key, limit)
        if key == a.key:
            raise ValueError("offline rollback")
        return result

    monkeypatch.setattr(retention, "_ordinary", fail_first)
    report = cache.prune(RetentionPolicy(("ccxt",), 1, 0))
    assert report["status"] == "partial" and len(report["errors"]) == 1
    assert cache.read_latest_summary(a.key).count == 3
    assert cache.read_latest_summary(b.key).count == 1
    assert report["ohlcv"]["deleted_rows"] == 2


def test_prune_keeps_exact_revised_node_and_removes_only_unreferenced_context(cache):
    original = mapping()
    cache.submit_mapping(original)
    revised = replace(original.nodes[0], date=date(2026, 8, 1))
    cache.submit_mapping(
        replace(
            original,
            records=[MappingDay(date(2026, 9, 3), revised.underlying_symbol)],
            nodes=[revised],
        )
    )
    report = cache.prune(RetentionPolicy(("tq",), 30000, 0, 10), date(2026, 9, 3))
    assert report["status"] == "completed"
    result = cache.read_mapping_range(
        original.symbol, [date(2026, 9, 3)], original.facts.verified_date
    )
    assert result is not None and result.nodes == [revised]
    assert cache._connection().execute(
        "SELECT COUNT(*) FROM mapping_context WHERE roll_date=?",
        [original.nodes[0].date],
    ).fetchone() == (0,)


def test_structural_failure_reports_previously_committed_work(cache, monkeypatch):
    import duckdb

    from src.cache_tool import retention

    a, b = series(symbol="AAA"), series(symbol="BBB")
    for item in (a, b):
        cache.write_segment(item.key, rows(1, 2, 3), 1)
    original = retention._ordinary

    def fail_second(cache_, connection, key, limit):
        if key == b.key:
            raise duckdb.CatalogException("offline missing table")
        return original(cache_, connection, key, limit)

    monkeypatch.setattr(retention, "_ordinary", fail_second)
    with pytest.raises(MaintenanceFailure) as error:
        cache.prune(RetentionPolicy(("ccxt",), 1, 0))
    assert error.value.report["ohlcv"] == {"series_processed": 1, "deleted_rows": 2}
    assert cache.read_latest_summary(a.key).count == 1
    assert cache.read_latest_summary(b.key).count == 3


def test_year_retention_uses_calendar_years():
    assert years_before(date(2024, 2, 29), 1) == date(2023, 2, 28)
    assert years_before(date(2026, 9, 26), 10) == date(2016, 9, 26)


def test_close_finishes_current_identity_and_stops_the_next(cache, monkeypatch):
    from src.cache_tool import retention

    for symbol in ("AAA", "BBB"):
        cache.write_segment(series(symbol=symbol).key, rows(1, 2, 3), 1)
    entered, release = threading.Event(), threading.Event()
    original = retention._ordinary

    def paused(*args):
        result = original(*args)
        entered.set()
        assert release.wait(3)
        return result

    monkeypatch.setattr(retention, "_ordinary", paused)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pruning = pool.submit(cache.prune, RetentionPolicy(("ccxt",), 1, 0))
        assert entered.wait(3)
        closing = pool.submit(cache.close)
        with cache._lifecycle:
            assert cache._lifecycle.wait_for(lambda: cache._closing, timeout=3)
        release.set()
        report = pruning.result(timeout=3)
        closing.result(timeout=3)
    assert report["ohlcv"] == {"series_processed": 1, "deleted_rows": 2}
    assert report["status"] == "partial"
    assert report["errors"][-1]["code"] == "CACHE_MAINTENANCE_STOPPED"
