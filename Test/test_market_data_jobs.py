import asyncio
from urllib.parse import parse_qs

import httpx
import pytest
from loguru import logger

from src.tools import market_data_http
from src.tools.market_data_http import JobRequestError, MarketDataHttp
from src.tools.market_data_pipeline import run_once
from src.tools.market_data_types import MarketDataPlan
from Test.test_market_data_config import job_config

TOKEN = {"access_token": "private-token", "token_type": "bearer", "expires_in": 3600}
PRUNE = {
    "status": "completed",
    "errors": [],
    "auxiliary": {"status": "completed"},
    "ohlcv": {"deleted": 0},
}


def plan(**collection):
    return MarketDataPlan.model_validate(
        {"tq_collection": {"symbols": {"ma": "KQ.m@CZCE.MA"}, **collection}}
    )


def mock_client(handler):
    async def transport(request):
        if request.url.path == "/auth/token":
            assert parse_qs(request.content.decode()) == {
                "username": ["worker"],
                "password": ["do-not-log-password"],
            }
            return httpx.Response(200, json=TOKEN)
        assert request.headers["authorization"] == "Bearer private-token"
        return handler(request)

    return MarketDataHttp(job_config(), transport=httpx.MockTransport(transport))


def test_pipeline_fixed_requests_actual_bounds_calendar_once_and_prune_last():
    seen = []
    first, last = 1700000000000000000, 1700086400000000000

    def handler(request):
        seen.append(request)
        path = request.url.path
        if path == "/tq/fetch_ohlcv":
            assert request.url.params["data_length"] == "10000"
            assert request.url.params["enable_cache"] == "true"
            return httpx.Response(200, json=[{"datetime": first}, {"datetime": last}])
        if path == "/tq/fetch_underlying_symbol":
            assert dict(request.url.params) == {
                "symbol": "KQ.m@CZCE.MA",
                "start_time": str(first),
                "end_time": str(last),
                "enable_cache": "true",
                "transition_timeframe": "5m",
            }
            return httpx.Response(200, json={"items": [], "history": []})
        if path == "/system/fetch_time":
            return httpx.Response(200, json={"serverTime": 1709164800000})
        if path == "/tq/fetch_trading_calendar":
            assert dict(request.url.params) == {
                "start_date": "2014-02-28",
                "end_date": "2024-02-29",
                "enable_cache": "true",
            }
            return httpx.Response(200, json=[])
        assert path == "/cache/prune"
        import json

        assert json.loads(request.content) == {
            "providers": ["tq", "ccxt"],
            "modes": {"live": 30000, "sandbox": 0},
            "auxiliary": {"keep_years": 10},
        }
        return httpx.Response(200, json=PRUNE)

    async def run():
        client = mock_client(handler)
        try:
            result = await run_once(client, plan(), job_config())
            assert result.failed == 0 and result.succeeded == 11
        finally:
            await client.close()

    asyncio.run(run())
    assert len(seen) == 12 and seen[-1].url.path == "/cache/prune"
    assert [
        r.url.params["symbol"] for r in seen if r.url.path == "/tq/fetch_ohlcv"
    ] == ["KQ.m@CZCE.MA", "KQ.i@CZCE.MA"] * 4
    assert sum(r.url.path == "/tq/fetch_underlying_symbol" for r in seen) == 1


@pytest.mark.parametrize("mode", ["empty", "failed", "no_transition", "no_mapping"])
def test_short_empty_failure_and_switches_do_not_probe_or_fallback_to_other_period(
    mode,
):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.path == "/tq/fetch_ohlcv":
            is_main5 = (
                request.url.params["symbol"].startswith("KQ.m@")
                and request.url.params["duration_seconds"] == "300"
            )
            if is_main5 and mode == "failed":
                return httpx.Response(504, json={"detail": {"code": "TQ_DATA_TIMEOUT"}})
            return httpx.Response(
                200,
                json=[]
                if is_main5 and mode == "empty"
                else [{"datetime": 1700000000000000000}],
            )
        if request.url.path == "/tq/fetch_underlying_symbol":
            assert "transition_timeframe" not in request.url.params
            return httpx.Response(200, json={"history": []})
        return httpx.Response(200, json=PRUNE)

    async def run():
        client = mock_client(handler)
        try:
            result = await run_once(
                client,
                plan(
                    save_calendar=False,
                    save_transition=False,
                    save_mapping=mode != "no_mapping",
                ),
                job_config(),
            )
            assert result.failed == int(mode == "failed")
        finally:
            await client.close()

    asyncio.run(run())
    assert len([r for r in calls if r.url.path == "/tq/fetch_ohlcv"]) == 8
    assert sum(r.url.path == "/tq/fetch_underlying_symbol" for r in calls) == int(
        mode == "no_transition"
    )
    assert calls[-1].url.path == "/cache/prune"


@pytest.mark.parametrize(
    "status,body,failed,skipped",
    [(409, {}, 0, 2), (200, {**PRUNE, "status": "partial"}, 1, 1), (500, {}, 1, 1)],
)
def test_only_prune_works_without_tq_and_does_not_retry(status, body, failed, skipped):
    calls = []

    def handler(request):
        calls.append(request)
        assert b'"auxiliary":null' in request.content
        return httpx.Response(status, json=body)

    async def run():
        client = mock_client(handler)
        configured = plan()
        configured.retention.providers = ["ccxt"]
        try:
            result = await run_once(client, configured, job_config(tq=False))
            assert (result.failed, result.skipped) == (failed, skipped)
        finally:
            await client.close()

    asyncio.run(run())
    assert len(calls) == 1


def test_tokens_expire_and_401_reauth_only_once(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(market_data_http, "monotonic", lambda: now[0])
    calls = []

    async def handler(request):
        calls.append(request.url.path)
        if request.url.path == "/auth/token":
            return httpx.Response(200, json=TOKEN)
        if request.url.path == "/fail":
            return httpx.Response(401, json={})
        return httpx.Response(200, json=[])

    async def run():
        client = MarketDataHttp(job_config(), transport=httpx.MockTransport(handler))
        try:
            await client.request("GET", "/ok")
            now[0] = 3600
            await client.request("GET", "/ok")
            with pytest.raises(JobRequestError) as error:
                await client.request("POST", "/fail")
            assert error.value.status == 401
        finally:
            await client.close()

    asyncio.run(run())
    assert calls == [
        "/auth/token",
        "/ok",
        "/auth/token",
        "/ok",
        "/fail",
        "/auth/token",
        "/fail",
    ]


def test_timeout_never_resends_post_and_logs_are_redacted():
    calls, logs = [], []

    def handler(request):
        calls.append(request.url.path)
        raise httpx.ReadTimeout("do-not-log-password private-token", request=request)

    async def run():
        client = mock_client(handler)
        try:
            result = await run_once(client, plan(), job_config(tq=False))
            assert result.failed == 1
        finally:
            await client.close()

    sink = logger.add(logs.append, format="{message} {extra}")
    try:
        asyncio.run(run())
    finally:
        logger.remove(sink)
    assert calls == ["/cache/prune"]
    assert "BACKGROUND_HTTP_TIMEOUT" in "".join(logs)
    assert "do-not-log-password" not in "".join(
        logs
    ) and "private-token" not in "".join(logs)
