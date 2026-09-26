import asyncio

import pandas as pd
import pytest
from fastapi import HTTPException

from src.cache_tool import TqOhlcvBatch, TqOhlcvSeries
from src.responses_time import PublicTimeResponse
from src.responses_tq import TqTradingStatusResponse
from src.tools.tq_metadata_conversion import parse_holidays, parse_mapping
from src.types_tq import TqOhlcvRequest
from Test.test_tq_metadata_conversion import HOLIDAYS, ns
from Test.test_tq_ohlcv_cache import SYMBOL, records
from Test.test_tq_ohlcv_cache import service as service


@pytest.fixture
def history_fallback(service, monkeypatch):
    manager, api, cache = service
    checked = []

    def serial(symbol, duration, length, adj_type=None):
        assert api.loop.is_running()
        api.calls.append((symbol, duration, length))
        if duration != 300:
            raise HTTPException(502, "TQ_NETWORK_UNAVAILABLE")
        return pd.DataFrame([dict(records()[0], datetime=ns("2026-09-24T14:55:00"))])

    async def headers():
        return {}

    async def clock():
        return PublicTimeResponse(serverTime=ns("2026-09-26T13:00:00") // 1_000_000)

    async def source(kind, headers):
        if kind == "calendar":
            return parse_holidays(HOLIDAYS)
        return parse_mapping(
            {"SHFE.rb": [["20260818", "SHFE.rb2610"], ["20260924", "SHFE.rb2611"]]}
        )

    def status(symbol):
        checked.append(symbol)
        return TqTradingStatusResponse(
            symbol=symbol, raw_status="NOTRADING", is_open=False
        )

    monkeypatch.setattr(api, "get_kline_serial", serial)
    monkeypatch.setattr(manager, "_metadata_headers", headers)
    monkeypatch.setattr("src.tools.tq_metadata_query.fetch_public_time", clock)
    query = manager._metadata_query()
    monkeypatch.setattr(query.source, "fetch", source)
    monkeypatch.setattr(manager._client.status_snapshot, "read", status)
    return manager, api, cache, checked


@pytest.mark.parametrize("symbol", [SYMBOL, "KQ.i@SHFE.rb"])
def test_closed_fallback_uses_history_source_without_quote_mapping(
    history_fallback, symbol
):
    manager, api, cache, checked = history_fallback
    cache.write_tq_segment(TqOhlcvSeries(symbol, 60), TqOhlcvBatch(records()))

    async def run():
        try:
            result = await manager.fetch_ohlcv(
                TqOhlcvRequest(symbol=symbol, duration_seconds=60)
            )
            assert [row["id"] for row in result] == [0, 1]
            assert checked == ["SHFE.rb2611"]
            assert api.calls == [(symbol, 60, 10000), (SYMBOL, 300, 1)]
        finally:
            await manager.close_metadata()

    asyncio.run(run())


@pytest.mark.parametrize(
    "status,detail",
    [(504, "TQ_METADATA_SOURCE_TIMEOUT"), (403, "TQ_PERMISSION_DENIED")],
)
def test_mapping_failure_never_guesses_closed_status(
    history_fallback, monkeypatch, status, detail
):
    manager, _, _, checked = history_fallback

    async def failure(kind, headers):
        raise HTTPException(status, detail)

    monkeypatch.setattr(manager._metadata.source, "fetch", failure)

    async def run():
        try:
            with pytest.raises(HTTPException) as error:
                await manager.fetch_ohlcv(
                    TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60)
                )
            expected = (
                (403, detail)
                if status == 403
                else (502, "TQ_TRADING_STATUS_UNAVAILABLE")
            )
            assert (error.value.status_code, error.value.detail) == expected
            assert checked == []
        finally:
            await manager.close_metadata()

    asyncio.run(run())


def test_closed_cache_read_failure_and_status_permission_are_errors(
    service, monkeypatch
):
    manager, api, cache = service
    api.error = HTTPException(502, "TQ_NETWORK_UNAVAILABLE")
    request = TqOhlcvRequest(symbol="SHFE.rb2610", duration_seconds=60)
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda symbol: TqTradingStatusResponse(
            symbol=symbol, raw_status="NOTRADING", is_open=False
        ),
    )
    assert asyncio.run(manager.fetch_ohlcv(request)) == []

    def fail_read(*args):
        raise RuntimeError("offline database failure")

    monkeypatch.setattr(cache, "read_contiguous_before", fail_read)
    with pytest.raises(HTTPException) as error:
        asyncio.run(manager.fetch_ohlcv(request))
    assert (error.value.status_code, error.value.detail) == (
        500,
        "TQ_CACHE_READ_FAILED",
    )

    def denied(*args):
        raise HTTPException(403, "TQ_TRADING_STATUS_PERMISSION_DENIED")

    monkeypatch.setattr(manager._client.status_snapshot, "read", denied)
    with pytest.raises(
        HTTPException, match="TQ_TRADING_STATUS_PERMISSION_DENIED"
    ) as error:
        asyncio.run(manager.fetch_ohlcv(request))
    assert error.value.status_code == 403
