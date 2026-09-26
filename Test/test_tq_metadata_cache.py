from dataclasses import replace
from datetime import date

import pytest

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult
from src.cache_tool.metadata_models import (
    CalendarDay,
    CalendarFacts,
    CalendarSourceResult,
    MappingDay,
    MappingFacts,
    MappingNode,
    MappingSourceResult,
    calendar_dates,
)


@pytest.fixture
def cache(tmp_path):
    result = DuckDbOhlcvCache(tmp_path / "metadata.duckdb", 100_001, 200_000)
    yield result
    result.close()


def calendar(start, end):
    start, end = date(2026, 9, start), date(2026, 9, end)
    return CalendarSourceResult(
        [CalendarDay(day, day.weekday() < 5) for day in calendar_dates(start, end)],
        CalendarFacts(
            date(2026, 10, 7),
            date(2010, 1, 1),
            date(2026, 12, 31),
            "holiday",
            1790000000000,
        ),
    )


def test_calendar_only_actual_date_overlap_connects_and_range_must_be_complete(cache):
    cache.submit_calendar(calendar(1, 3))
    cache.submit_calendar(calendar(4, 6))
    assert cache.read_calendar_range(date(2026, 9, 1), date(2026, 9, 6)) is None
    cache.submit_calendar(calendar(3, 4))
    result = cache.read_calendar_range(date(2026, 9, 1), date(2026, 9, 6))
    assert result is not None and len(result.records) == 6
    assert result.records[-1].trading is False
    # 普通行情维护不能删除日期片段。
    cache.write_segment(
        "ohlcv", OhlcvResult([(1, 1.0, 2.0, 0.0, 1.0, 1.0)], True), None
    )
    assert cache.read_calendar_range(date(2026, 9, 1), date(2026, 9, 6)) is not None


def test_invalid_calendar_and_write_failure_do_not_advance_facts(cache, monkeypatch):
    original = calendar(1, 3)
    cache.submit_calendar(original)
    with pytest.raises(ValueError, match="incomplete"):
        cache.submit_calendar(replace(original, records=original.records[::2]))
    original_facts = cache.read_metadata_facts("calendar")
    from src.cache_tool import metadata_store

    def fail(*args):
        raise RuntimeError("offline fact failure")

    monkeypatch.setattr(metadata_store, "_save_facts", fail)
    with pytest.raises(RuntimeError, match="fact failure"):
        cache.submit_calendar(calendar(3, 6))
    assert cache.read_calendar_range(date(2026, 9, 1), date(2026, 9, 6)) is None
    assert cache.read_metadata_facts("calendar") == original_facts


def mapping():
    symbol = "KQ.m@SHFE.rb"
    node = MappingNode(date(2026, 8, 10), "SHFE.rb2610", "SHFE.rb2605")
    rows = [MappingDay(date(2026, 9, day), "SHFE.rb2610") for day in (1, 2, 3)]
    current = MappingNode(date(2026, 9, 21), "SHFE.rb2701", "SHFE.rb2610")
    facts = MappingFacts(
        "mapping",
        "holiday",
        1790410000000,
        date(2026, 9, 24),
        "SHFE.rb2701",
        1790211600000000000,
    )
    return MappingSourceResult(symbol, rows, [node], current, facts)


def test_old_range_keeps_true_left_node_and_current_day_does_not_fill_gap(cache):
    result = mapping()
    cache.submit_mapping(result)
    dates = [row.date for row in result.records]
    read = cache.read_mapping_range(
        result.symbol, dates[1:], result.facts.verified_date
    )
    assert read is not None and read.nodes == result.nodes
    assert read.nodes[0].date < dates[0]
    assert (
        cache.read_mapping_range(
            result.symbol, [date(2026, 9, 10)], result.facts.verified_date
        )
        is None
    )
    current = cache.read_mapping_range(
        result.symbol, [result.facts.verified_date], result.facts.verified_date
    )
    assert (
        current is not None
        and current.records[0].underlying_symbol == result.facts.underlying_symbol
    )
    assert cache._connection().execute(
        "SELECT COUNT(*) FROM cache_segments WHERE data_kind='main_mapping'"
    ).fetchone() == (2,)


def test_mapping_cannot_persist_or_read_future_history(cache):
    result = mapping()
    with pytest.raises(ValueError, match="verified date"):
        cache.submit_mapping(
            replace(result, records=[MappingDay(date(2026, 9, 28), "SHFE.rb2701")])
        )
    with pytest.raises(ValueError, match="verified date"):
        cache.submit_mapping(
            replace(
                result,
                verification_node=MappingNode(
                    date(2026, 9, 28), "SHFE.rb2701", "SHFE.rb2610"
                ),
            )
        )
    assert cache.read_metadata_facts("main_mapping", result.symbol) is None
    cache.submit_mapping(result)
    with pytest.raises(ValueError, match="verified boundary"):
        cache.read_mapping_range(
            result.symbol, [date(2026, 9, 28)], result.facts.verified_date
        )


def test_revised_daily_rows_keep_their_real_node_instead_of_stale_context(cache):
    result = mapping()
    a, b = "SHFE.rb2610", "SHFE.rb2701"
    def day(d):
        return date(2026, 9, d)
    nodes = [
        MappingNode(day(1), a, None),
        MappingNode(day(3), b, a),
        MappingNode(day(5), a, b),
    ]
    initial = replace(
        result,
        records=[MappingDay(day(d), a if d < 3 or d >= 5 else b) for d in range(1, 6)],
        nodes=nodes,
        verification_node=nodes[-1],
        facts=replace(result.facts, verified_date=day(6), underlying_symbol=a),
    )
    cache.submit_mapping(initial)
    revised = replace(
        initial,
        records=[MappingDay(day(5), a)],
        nodes=nodes[:1],
        verification_node=nodes[0],
    )
    cache.submit_mapping(revised)
    tail = cache.read_mapping_range(result.symbol, [day(5)], day(6))
    assert tail is not None and tail.nodes == nodes[:1]
    assert (
        cache.read_mapping_range(result.symbol, [day(d) for d in range(1, 6)], day(6))
        is None
    )
    cache.submit_mapping(
        replace(revised, records=[MappingDay(day(d), a) for d in range(1, 6)])
    )
    whole = cache.read_mapping_range(
        result.symbol, [day(d) for d in range(1, 6)], day(6)
    )
    assert whole is not None and whole.nodes == nodes[:1]


def test_partial_revision_of_same_node_date_does_not_reassign_untouched_rows(cache):
    original = mapping()
    cache.submit_mapping(original)
    revised_node = replace(original.nodes[0], underlying_symbol="SHFE.rb2609")
    cache.submit_mapping(
        replace(
            original,
            records=[
                MappingDay(original.records[0].date, revised_node.underlying_symbol)
            ],
            nodes=[revised_node],
        )
    )
    revised = cache.read_mapping_range(
        original.symbol, [original.records[0].date], original.facts.verified_date
    )
    untouched = cache.read_mapping_range(
        original.symbol, [original.records[1].date], original.facts.verified_date
    )
    assert revised is not None and revised.nodes == [revised_node]
    assert untouched is not None and untouched.nodes == original.nodes
    assert (
        cache.read_mapping_range(
            original.symbol,
            [r.date for r in original.records],
            original.facts.verified_date,
        )
        is None
    )
