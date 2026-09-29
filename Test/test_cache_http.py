import asyncio
import threading
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult, OhlcvSeries
from src.router.auth_handler import manager as auth_manager
from src.router.cache_router import cache_router

BODY = {"providers": ["tq", "ccxt"], "modes": {"live": 1, "sandbox": 0}}


@pytest.fixture
def app(tmp_path):
    cache = DuckDbOhlcvCache(tmp_path / "http.duckdb", 100_001, 200_000)
    key = OhlcvSeries("binance", "live", "future", "BTC/USDT", "1m").key
    cache.write_segment(
        key, OhlcvResult([(t, 1.0, 2.0, 0.0, 1.0, 1.0) for t in (1, 2, 3)], True), 1
    )
    app = FastAPI()
    app.include_router(cache_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    app.state.service_runtime = SimpleNamespace(
        ready=True,
        cache=SimpleNamespace(get=lambda: cache),
        cache_maintenance_lock=threading.Lock(),
    )
    yield app, cache
    cache.close()


async def call(app, method, path, **kwargs):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://offline"
    ) as client:
        return await client.request(method, path, **kwargs)


def test_summary_is_local_and_prune_rules_are_explicit(app, monkeypatch):
    application, _ = app

    async def no_clock():
        pytest.fail("概况或仅根数清理不应取时")

    monkeypatch.setattr("src.router.cache_router.fetch_public_time", no_clock)

    async def run():
        response = await call(application, "GET", "/cache/summary?is_live=true")
        assert response.status_code == 200
        assert response.json()["items"][0]["total_count"] == 3
        assert set(response.json()["items"][0]) == {
            "kind",
            "identity",
            "time_unit",
            "start",
            "end",
            "count",
            "total_count",
            "segment_count",
        }
        result = await call(application, "POST", "/cache/prune", json=BODY)
        assert result.status_code == 200
        assert result.json()["rules"] == BODY | {"auxiliary": None}
        assert result.json()["ohlcv"]["deleted_rows"] == 2
        assert result.json()["auxiliary"]["status"] == "not_requested"
        assert (
            await call(application, "GET", "/cache/summary?is_live=true&symbol=missing")
        ).json() == {"items": []}

    asyncio.run(run())


def test_online_clock_failure_skips_auxiliary_but_still_prunes_rows(app, monkeypatch):
    application, cache = app

    async def fail():
        raise HTTPException(504, {"code": "PUBLIC_TIME_TIMEOUT"})

    monkeypatch.setattr("src.router.cache_router.fetch_public_time", fail)
    result = asyncio.run(
        call(
            application,
            "POST",
            "/cache/prune",
            json=BODY | {"auxiliary": {"keep_years": 10}},
        )
    )
    assert result.status_code == 200
    data = result.json()
    assert data["status"] == "partial"
    assert data["auxiliary"] == {
        "status": "skipped",
        "cutoff_date": None,
        "deleted_rows": 0,
    }
    assert data["errors"] == [{"scope": "auxiliary", "code": "PUBLIC_TIME_TIMEOUT"}]
    assert cache.list_series_summaries()[0]["total_count"] == 1


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"providers": [], "modes": {"live": 0, "sandbox": 0}},
        {"providers": ["ccxt"], "modes": {"live": True, "sandbox": 0}},
        {"providers": ["ccxt"], "modes": {"live": 0}},
        {
            "providers": ["ccxt"],
            "modes": {"live": 0, "sandbox": 0},
            "auxiliary": {"keep_years": 10},
        },
        BODY | {"now": 0},
    ],
)
def test_invalid_policy_is_rejected_before_database_or_time(app, body, monkeypatch):
    application, cache = app
    monkeypatch.setattr(
        cache, "prune", lambda *args: pytest.fail("非法请求不得进入清理")
    )
    response = asyncio.run(call(application, "POST", "/cache/prune", json=body))
    assert response.status_code == 422


def test_busy_guard_is_held_until_cancelled_https_database_work_finishes(
    app, monkeypatch
):
    application, cache = app
    entered, release = threading.Event(), threading.Event()
    original = cache.prune

    def blocking(*args):
        entered.set()
        assert release.wait(3)
        return original(*args)

    monkeypatch.setattr(cache, "prune", blocking)

    async def run():
        first = asyncio.create_task(
            call(application, "POST", "/cache/prune", json=BODY)
        )
        assert await asyncio.to_thread(entered.wait, 2)
        first.cancel()
        await asyncio.sleep(0)
        try:
            second = await call(application, "POST", "/cache/prune", json=BODY)
            assert second.status_code == 409
            assert second.json()["detail"]["code"] == "CACHE_MAINTENANCE_BUSY"
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert not application.state.service_runtime.cache_maintenance_lock.locked()

    asyncio.run(run())


def test_disabled_application_refuses_inspection(app):
    application, _ = app
    application.state.service_runtime.ready = False
    response = asyncio.run(call(application, "GET", "/cache/summary?is_live=true"))
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "CACHE_NOT_READY"
