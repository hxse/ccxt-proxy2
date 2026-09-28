import asyncio

import httpx
import pytest

from src.tools import market_data_pipeline as jobs
from src.tools.config_loader import ConfigError
from src.tools.market_data_http import JobRequestError, MarketDataHttp
from src.tools.market_data_types import MarketDataPlan
from src.tools.service_runtime import ServiceRuntime
from Test.test_market_data_config import job_config
from Test.test_market_data_jobs import plan
from Test.test_service_lifecycle import fake_managers


def test_scheduler_skips_missed_ticks_and_closes_http_on_cancel(monkeypatch):
    events, clock = [], [0.0]
    paused = asyncio.Event()

    class Client:
        def __init__(self, config):
            events.append("client")

        async def wait_ready(self):
            events.append("ready")

        async def close(self):
            events.append("close")

    async def once(*args):
        events.append("round")
        clock[0] += 25

    async def sleep(delay):
        events.append(delay)
        paused.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(jobs, "MarketDataHttp", Client)
    monkeypatch.setattr(jobs, "monotonic", lambda: clock[0])
    monkeypatch.setattr(jobs, "run_once", once)
    monkeypatch.setattr(jobs.asyncio, "sleep", sleep)

    async def run():
        configured = plan()
        configured.pipeline.interval_seconds = 10
        scheduler = jobs.MarketDataScheduler(configured, job_config())
        scheduler.start()
        scheduler.start()
        await paused.wait()
        await scheduler.close()
        await scheduler.close()

    asyncio.run(run())
    assert events == ["client", "ready", "round", 5.0, "close"]


def test_ready_wait_is_bounded_and_does_not_login():
    paths = []

    async def unavailable(request):
        paths.append(request.url.path)
        return httpx.Response(503)

    async def run():
        client = MarketDataHttp(
            job_config(), transport=httpx.MockTransport(unavailable)
        )
        try:
            with pytest.raises(JobRequestError, match="BACKGROUND_SERVICE_NOT_READY"):
                await client.wait_ready(timeout=0.02)
        finally:
            await client.close()

    asyncio.run(run())
    assert paths == ["/readyz"]


def test_invalid_background_account_fails_before_any_sdk_initialization(
    tmp_path, monkeypatch
):
    config = job_config()
    config.market_data_client.user = None
    config.ohlcv_cache.database_path = str(tmp_path / "cache.duckdb")
    monkeypatch.setattr("src.tools.service_runtime.load_market_data_plan", plan)
    runtime = ServiceRuntime(config)
    events = []
    with pytest.raises(ConfigError, match="user"):
        runtime.start(*fake_managers(events))
    assert not runtime.ready and events == []


def test_disabled_plan_starts_no_client_and_cli_returns_zero(monkeypatch):
    configured = MarketDataPlan.model_validate(
        {"tq_collection": {"enabled": False}, "retention": {"enabled": False}}
    )
    monkeypatch.setattr(
        jobs,
        "MarketDataHttp",
        lambda *_: pytest.fail("disabled plan created HTTP client"),
    )
    monkeypatch.setattr(jobs, "load_market_data_plan", lambda: configured)
    monkeypatch.setattr(jobs, "load_config", job_config)

    async def run():
        scheduler = jobs.MarketDataScheduler(configured, job_config())
        scheduler.start()
        assert scheduler._task is None
        await scheduler.close()
        assert await jobs.run_cli() == 0

    asyncio.run(run())


def test_lifespan_starts_without_waiting_for_self_http_and_stops_jobs_first(
    tmp_path, monkeypatch
):
    from fastapi import FastAPI

    from src.tools.shared import lifespan

    config = job_config()
    config.ohlcv_cache.database_path = str(tmp_path / "cache.duckdb")
    runtime = ServiceRuntime(config)
    events = []
    ccxt, tq, ctp = fake_managers(events)
    waiting = asyncio.Event()

    class Client:
        def __init__(self, _):
            pass

        async def wait_ready(self):
            assert runtime.ready
            waiting.set()
            await asyncio.Event().wait()

        async def close(self):
            events.append("close:jobs")

    monkeypatch.setattr("src.tools.service_runtime.load_market_data_plan", plan)
    monkeypatch.setattr(jobs, "MarketDataHttp", Client)
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", ccxt)
    monkeypatch.setattr("src.tools.tq_manager.tq_manager", tq)
    monkeypatch.setattr("src.tools.ctp_manager.ctp_manager", ctp)

    async def run():
        async with lifespan(FastAPI()):
            await asyncio.wait_for(waiting.wait(), 1)
            await asyncio.to_thread(runtime.wait_for_startup)
            assert runtime.ready

    asyncio.run(run())
    assert events == ["init:tq", "close:jobs", "close:tq"]
