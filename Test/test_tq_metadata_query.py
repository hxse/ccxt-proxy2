import asyncio
from dataclasses import replace
from datetime import date
from typing import Any

import httpx
import pytest
from fastapi import HTTPException

from src.cache_tool import DuckDbOhlcvCache
from src.responses_time import PublicTimeResponse
from src.tools.tq_metadata_conversion import parse_holidays, parse_mapping
from src.tools.tq_metadata_query import TqMetadataQuery
from src.tools.tq_metadata_source import MetadataSource
from src.types_tq import TqTradingCalendarRequest, TqUnderlyingSymbolRequest
from Test.test_tq_metadata_conversion import EVENTS, HOLIDAYS, SYMBOL, ns


@pytest.fixture
def query(tmp_path, monkeypatch):
    state: dict[str, Any] = {
        "time": ns("2026-09-26T13:00:00") // 1_000_000,
        "reference": ns("2026-09-24T14:55:00"),
        "calls": [],
        "clocks": 0,
        "references": 0,
        "mapping": EVENTS,
    }
    cache = DuckDbOhlcvCache(tmp_path / "query.duckdb", 100_001, 200_000)

    async def clock():
        state["clocks"] += 1
        return PublicTimeResponse(serverTime=state["time"])

    async def headers():
        return {"offline": "fixture"}

    async def reference(symbol):
        state["references"] += 1
        return state["reference"]

    async def fetch(kind, headers):
        state["calls"].append(kind)
        return (
            parse_holidays(HOLIDAYS)
            if kind == "calendar"
            else parse_mapping(state["mapping"])
        )

    service = TqMetadataQuery(cache, headers, reference)
    monkeypatch.setattr("src.tools.tq_metadata_query.fetch_public_time", clock)
    monkeypatch.setattr(service.source, "fetch", fetch)
    yield service, state, cache
    cache.close()


def historical():
    return TqUnderlyingSymbolRequest(
        symbol=SYMBOL,
        start_time=ns("2026-09-23T09:00:00"),
        end_time=ns("2026-09-24T14:55:00"),
    )


def test_calendar_uses_actual_h_refresh_boundary_and_one_online_clock(query):
    service, state, _ = query
    request = TqTradingCalendarRequest(
        start_date=date(2026, 9, 23), end_date=date(2026, 9, 26)
    )

    async def run():
        first = await service.calendar(request)
        assert await service.calendar(request) == first
        assert state["calls"] == ["calendar"] and state["clocks"] == 2
        state["time"] = ns("2026-10-07T12:00:00") // 1_000_000
        assert await service.calendar(request) == first
        assert state["calls"] == ["calendar", "calendar"]
        # 新源 H 未延长，单次不循环；下一次请求仍按条件刷新。
        await service.calendar(request)
        assert state["calls"] == ["calendar"] * 3
        assert first[-1] == {"date": "2026-09-26", "trading": False}

    asyncio.run(run())


def test_mapping_downloads_on_full_cache_hit_and_advances_only_with_reference(query):
    service, state, cache = query

    async def run():
        first = await service.mapping(historical())
        second = await service.mapping(historical())
        assert first == second
        assert state["clocks"] == state["references"] == 2
        assert state["calls"] == ["mapping", "calendar"] * 2
        facts = cache.read_metadata_facts("main_mapping", SYMBOL)
        assert facts.verified_date == date(2026, 9, 24)
        assert first.history[-1].underlying_symbol == "SHFE.rb2610"
        assert all(node.date <= "2026-09-24" for node in first.history)
        # 真正进入新的交易日后，原覆盖不够，合约即使已公布也要重新核验。
        state["time"] = ns("2026-09-29T15:00:00") // 1_000_000
        state["reference"] = ns("2026-09-29T14:55:00")
        await service.mapping(historical())
        assert state["calls"] == ["mapping", "calendar"] * 3
        assert cache.read_metadata_facts("main_mapping", SYMBOL).verified_date == date(
            2026, 9, 29
        )

    asyncio.run(run())


def test_refreshed_calendar_facts_are_saved_even_when_mapping_is_a_cache_hit(query):
    service, state, cache = query

    async def run():
        expected = await service.mapping(historical())
        old = cache.read_calendar_range(date(2026, 9, 23), date(2026, 9, 24))
        assert old is not None
        # 模拟已经到旧 H，而新源延长 H；映射日记录及当前 U 无变化。
        cache.submit_calendar(
            replace(old, facts=replace(old.facts, holiday_last=date(2026, 9, 26)))
        )
        assert await service.mapping(historical()) == expected
        assert cache.read_metadata_facts("calendar").holiday_last == date(2026, 10, 7)
        assert await service.mapping(historical()) == expected
        assert state["calls"] == ["mapping", "calendar"] * 3

    asyncio.run(run())


def test_future_range_and_invalid_source_never_update_facts(query):
    service, state, cache = query

    async def run():
        await service.mapping(historical())
        old = cache.read_metadata_facts("main_mapping", SYMBOL)
        request = historical().model_copy(
            update={"end_time": ns("2026-09-28T10:00:00")}
        )
        with pytest.raises(HTTPException, match="TQ_MAPPING_RANGE_UNAVAILABLE"):
            await service.mapping(request)
        state["mapping"] = {"SHFE.rb": []}
        with pytest.raises(HTTPException, match="TQ_METADATA_INVALID_SOURCE"):
            await service.mapping(historical())
        assert state["calls"].count("mapping") == 3
        assert cache.read_metadata_facts("main_mapping", SYMBOL) == old

    asyncio.run(run())


def test_disabled_cache_does_not_read_old_facts_or_write(query, monkeypatch):
    service, state, cache = query

    def forbidden(*args):
        pytest.fail("缓存关闭不得读写")

    for name in (
        "read_metadata_facts",
        "read_calendar_range",
        "read_mapping_range",
        "read_matching_mapping",
        "submit_calendar",
        "submit_mapping",
    ):
        monkeypatch.setattr(cache, name, forbidden)
    asyncio.run(
        service.mapping(historical().model_copy(update={"enable_cache": False}))
    )
    assert state["calls"] == ["mapping", "calendar"]


def test_clock_failure_cannot_be_hidden_by_cache_hit(query, monkeypatch):
    service, _, cache = query
    request = TqTradingCalendarRequest(
        start_date=date(2026, 9, 23), end_date=date(2026, 9, 24)
    )
    asyncio.run(service.calendar(request))

    async def fail_clock():
        raise HTTPException(504, {"code": "PUBLIC_TIME_TIMEOUT"})

    monkeypatch.setattr("src.tools.tq_metadata_query.fetch_public_time", fail_clock)
    with pytest.raises(HTTPException) as captured:
        asyncio.run(service.calendar(request))
    assert captured.value.status_code == 504
    assert captured.value.detail == {"code": "PUBLIC_TIME_TIMEOUT"}
    assert cache.read_calendar_range(request.start_date, request.end_date) is not None


def test_new_year_clock_does_not_require_future_calendar_for_valid_old_history(query):
    service, state, _ = query
    state["time"] = ns("2027-01-01T12:00:00") // 1_000_000
    result = asyncio.run(service.mapping(historical()))
    assert result.history[-1].underlying_symbol == "SHFE.rb2610"
    assert state["calls"] == ["mapping", "calendar"]


def test_source_download_only_coalesces_inflight_and_closes_client():
    async def run():
        source = MetadataSource()
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def transport(request):
            calls.append(request)
            entered.set()
            await release.wait()
            return httpx.Response(200, json=HOLIDAYS)

        source._client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
        first = asyncio.create_task(source.fetch("calendar", {}))
        await entered.wait()
        second = asyncio.create_task(source.fetch("calendar", {}))
        await asyncio.sleep(0)
        release.set()
        assert await first == await second
        assert len(calls) == 1
        await source.fetch("calendar", {})
        assert len(calls) == 2
        await source.close()
        assert source._client.is_closed
        with pytest.raises(HTTPException, match="TQ_NOT_READY"):
            await source.fetch("calendar", {})

    asyncio.run(run())


@pytest.mark.parametrize(
    "response,code",
    [
        (httpx.Response(503), "TQ_METADATA_SOURCE_UNAVAILABLE"),
        (httpx.Response(200, content="bad json"), "TQ_METADATA_INVALID_SOURCE"),
        (httpx.Response(200, json=[]), "TQ_METADATA_INVALID_SOURCE"),
    ],
)
def test_source_protocol_errors_are_stable(response, code):
    async def run():
        source = MetadataSource()
        source._client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: response)
        )
        try:
            with pytest.raises(HTTPException, match=code):
                await source.fetch("calendar", {})
        finally:
            await source.close()

    asyncio.run(run())
