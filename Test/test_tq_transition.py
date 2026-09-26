import asyncio
from datetime import date

import pytest
from fastapi import HTTPException

from src.cache_tool.transition_store import TransitionIdentity
from src.types_tq import TqUnderlyingSymbolRequest, tq_underlying_symbol_request
from Test.test_tq_metadata_conversion import SYMBOL, ns
from Test.test_tq_metadata_query import historical
from Test.test_tq_metadata_query import query as query
from Test.test_tq_requests import _query_request


def old_window(count):
    times = [
        ns("2026-09-23T14:55:00"),
        *[ns("2026-09-24T09:00:00") + i * 300_000_000_000 for i in range(count)],
    ]
    return [
        {
            "datetime": timestamp,
            "id": i,
            "open": 2.0,
            "high": 4.0,
            "low": 1.0,
            "close": 3.0,
            "volume": 10.0,
        }
        for i, timestamp in enumerate(times)
    ]


def request(count=10):
    return TqUnderlyingSymbolRequest.model_validate(
        historical().model_dump()
        | {"transition_timeframe": "5m", "transition_bars": count}
    )


def test_five_three_ten_use_raw_sdk_once_per_miss_and_only_persist_windows(query):
    service, _, cache = query
    calls = []

    async def raw_fetch(params, deadline):
        calls.append(params)
        return old_window(11)

    service.raw_fetch = raw_fetch

    async def run():
        first = await service.mapping(request(5))
        assert len(first.history[-1].transition) == 5
        second = await service.mapping(request(3))
        assert len(second.history[-1].transition) == 3
        assert len(calls) == 1
        third = await service.mapping(request(10))
        assert len(third.history[-1].transition) == 10
        assert len(calls) == 2
        assert all(
            call.symbol == "SHFE.rb2609"
            and call.data_length == 10000
            and call.enable_cache is False
            and call.adj_type is None
            for call in calls
        )
        assert cache._connection().execute(
            "SELECT COUNT(*) FROM ohlcv_rows"
        ).fetchone() == (0,)
        assert cache._connection().execute(
            "SELECT COUNT(*) FROM transition_rows"
        ).fetchone() == (10,)

    asyncio.run(run())


def test_insufficient_single_snapshot_never_joins_cached_partial_window(query):
    service, _, cache = query
    snapshots = [old_window(6), old_window(8)]

    async def raw_fetch(params, deadline):
        return snapshots.pop(0)

    service.raw_fetch = raw_fetch

    async def run():
        await service.mapping(request(5))
        result = await service.mapping(request(10))
        assert len(result.history[-1].transition) == 8
        key = TransitionIdentity(
            SYMBOL, date(2026, 9, 24), "SHFE.rb2610", "SHFE.rb2609", 300
        )
        assert len(cache.read_transition_prefix(key, 5)) == 5
        assert cache.read_transition_prefix(key, 6) is None
        assert snapshots == []

    asyncio.run(run())


@pytest.mark.parametrize("rows", [old_window(11)[1:], old_window(11)[3:], []])
def test_missing_predecessor_or_midday_truncation_returns_empty(query, rows):
    service, _, cache = query

    async def raw_fetch(params, deadline):
        return rows

    service.raw_fetch = raw_fetch
    result = asyncio.run(service.mapping(request()))
    assert result.history[-1].transition == []
    assert cache._connection().execute(
        "SELECT COUNT(*) FROM transition_rows"
    ).fetchone() == (0,)


def test_bad_proof_row_fails_instead_of_saving_first_ten(query):
    service, _, cache = query

    async def raw_fetch(params, deadline):
        rows = old_window(11)
        rows[-1]["volume"] = None
        return rows

    service.raw_fetch = raw_fetch
    with pytest.raises(HTTPException, match="TQ_INVALID_TRANSITION_WINDOW"):
        asyncio.run(service.mapping(request()))
    assert cache._connection().execute(
        "SELECT COUNT(*) FROM transition_rows"
    ).fetchone() == (0,)


def test_no_period_omits_transition_and_does_not_request_old_prices(query):
    service, _, _ = query

    async def raw_fetch(*args):
        pytest.fail("无周期不能取旧行情")

    service.raw_fetch = raw_fetch
    result = asyncio.run(service.mapping(historical()))
    assert all(
        "transition" not in node
        for node in result.model_dump(exclude_unset=True)["history"]
    )


def test_total_deadline_stops_old_price_work_without_partial_window(query, monkeypatch):
    service, _, cache = query
    calls = []

    async def raw_fetch(*args):
        calls.append(1)
        await asyncio.sleep(5)
        pytest.fail("超时后不能继续处理旧价格")

    service.raw_fetch = raw_fetch
    monkeypatch.setattr("src.tools.tq_metadata_query.TRANSITION_TIMEOUT_SECONDS", 0.2)

    async def run():
        await service.mapping(historical())
        with pytest.raises(HTTPException, match="TQ_TRANSITION_TIMEOUT") as error:
            await service.mapping(request())
        assert error.value.status_code == 504
        assert calls == [1]
        assert cache._connection().execute(
            "SELECT COUNT(*) FROM transition_rows"
        ).fetchone() == (0,)

    asyncio.run(run())


def test_upstream_failure_is_not_empty_transition(query):
    service, _, _ = query

    async def raw_fetch(*args):
        raise HTTPException(503, {"code": "SERVICE_NOT_READY", "service": "tq"})

    service.raw_fetch = raw_fetch
    with pytest.raises(HTTPException) as error:
        asyncio.run(service.mapping(request()))
    assert error.value.status_code == 503
    assert error.value.detail == {"code": "SERVICE_NOT_READY", "service": "tq"}


@pytest.mark.parametrize(
    "fields,code",
    [
        ({"transition_bars": 0}, "TQ_INVALID_TRANSITION_BARS"),
        ({"transition_bars": 10000}, "TQ_INVALID_TRANSITION_BARS"),
        ({"transition_timeframe": "1M"}, "TQ_INVALID_TRANSITION_TIMEFRAME"),
        ({"transition_timeframe": "25h"}, "TQ_INVALID_TRANSITION_TIMEFRAME"),
    ],
)
def test_transition_parameters_fail_before_fetch(fields, code):
    with pytest.raises(HTTPException, match=code) as error:
        tq_underlying_symbol_request(
            _query_request(),
            symbol=[SYMBOL],
            start_time=ns("2026-09-23T09:00:00"),
            end_time=ns("2026-09-24T14:55:00"),
            **fields,
        )
    assert error.value.status_code == 400
