import asyncio
from datetime import date

import pytest
from fastapi import HTTPException

from src.types_tq import TqUnderlyingSymbolRequest
from Test.test_tq_metadata_conversion import SYMBOL, ns
from Test.test_tq_metadata_query import historical
from Test.test_tq_metadata_query import query as query


def test_items_and_history_follow_fresh_source_and_ignore_future_nodes(query):
    service, state, cache = query

    async def run():
        first = await service.mapping(TqUnderlyingSymbolRequest(symbol=SYMBOL))
        assert first.items[0].underlying_symbol == "SHFE.rb2610"
        assert first.history == [] and "history" not in first.model_fields_set
        state["mapping"] = {
            "SHFE.rb": [
                ["20260701", "SHFE.rb2609"],
                ["20260924", "SHFE.rb2611"],
                ["20260928", "SHFE.rb2701"],
            ]
        }
        second = await service.mapping(historical())
        assert second.items[0].underlying_symbol == "SHFE.rb2611"
        assert second.history[-1].underlying_symbol == "SHFE.rb2611"
        assert second.history[-1].old_symbol == "SHFE.rb2609"
        assert state["calls"].count("mapping") == 2
        assert (
            cache.read_metadata_facts("main_mapping", SYMBOL).underlying_symbol
            == "SHFE.rb2611"
        )
        assert (
            cache.read_mapping_range(SYMBOL, [date(2026, 9, 28)], date(2026, 9, 28))
            is None
        )

    asyncio.run(run())


def test_identical_source_and_full_coverage_do_not_rewrite(query, monkeypatch):
    service, state, cache = query

    def forbidden(*args):
        pytest.fail("完整相同映射命中不能重复写回")

    async def run():
        expected = await service.mapping(historical())
        monkeypatch.setattr(cache, "submit_mapping", forbidden)
        monkeypatch.setattr(cache, "submit_calendar", forbidden)
        assert await service.mapping(historical()) == expected
        assert state["calls"].count("mapping") == 2

    asyncio.run(run())


def test_official_failure_cannot_be_hidden_by_items_cache(query, monkeypatch):
    service, _, cache = query

    async def unavailable(kind, headers):
        raise HTTPException(504, "TQ_METADATA_SOURCE_TIMEOUT")

    async def run():
        request = TqUnderlyingSymbolRequest(symbol=SYMBOL)
        await service.mapping(request)
        before = cache.read_metadata_facts("main_mapping", SYMBOL)
        monkeypatch.setattr(service.source, "fetch", unavailable)
        with pytest.raises(HTTPException) as captured:
            await service.mapping(request)
        assert (captured.value.status_code, captured.value.detail) == (
            504,
            "TQ_METADATA_SOURCE_TIMEOUT",
        )
        assert cache.read_metadata_facts("main_mapping", SYMBOL) == before

    asyncio.run(run())


def test_revised_digest_does_not_certify_another_old_segment(query):
    service, state, cache = query
    early = historical().model_copy(
        update={
            "start_time": ns("2026-09-01T09:00:00"),
            "end_time": ns("2026-09-03T14:55:00"),
        }
    )
    later = historical().model_copy(
        update={
            "start_time": ns("2026-09-21T09:00:00"),
            "end_time": ns("2026-09-23T14:55:00"),
        }
    )

    async def run():
        await service.mapping(early)
        await service.mapping(later)
        state["mapping"] = {
            "SHFE.rb": [["20260701", "SHFE.rb2608"], ["20260924", "SHFE.rb2610"]]
        }
        await service.mapping(early)
        # 全局摘要已更新，但另一独立片段仍是旧源的记录。
        dates = [date(2026, 9, d) for d in (21, 22, 23)]
        stale = cache.read_mapping_range(SYMBOL, dates, date(2026, 9, 24))
        assert stale is not None and stale.records[0].underlying_symbol == "SHFE.rb2609"
        repaired = await service.mapping(later)
        assert repaired.items[0].underlying_symbol == "SHFE.rb2610"
        assert repaired.history[0].underlying_symbol == "SHFE.rb2608"
        current = cache.read_mapping_range(SYMBOL, dates, date(2026, 9, 24))
        assert current is not None and all(
            row.underlying_symbol == "SHFE.rb2608" for row in current.records
        )

    asyncio.run(run())


def test_current_query_has_same_clock_reference_and_cache_switch(query, monkeypatch):
    service, state, cache = query

    def forbidden(*args):
        pytest.fail("enable_cache=false 不得读写本地缓存")

    for method in (
        "read_metadata_facts",
        "read_calendar_range",
        "read_matching_mapping",
        "submit_calendar",
        "submit_mapping",
    ):
        monkeypatch.setattr(cache, method, forbidden)
    result = asyncio.run(
        service.mapping(TqUnderlyingSymbolRequest(symbol=SYMBOL, enable_cache=False))
    )
    assert result.items[0].underlying_symbol == "SHFE.rb2610"
    assert state["clocks"] == state["references"] == 1


@pytest.mark.parametrize("symbol", ["SHFE.rb2610", "KQ.m@SHFE.unknown"])
def test_non_main_or_unknown_symbol_cannot_return_empty_success(query, symbol):
    service, _, _ = query
    with pytest.raises(HTTPException) as captured:
        asyncio.run(service.mapping(TqUnderlyingSymbolRequest(symbol=symbol)))
    assert (captured.value.status_code, captured.value.detail) == (
        422,
        "TQ_NOT_CONT_SYMBOL",
    )
