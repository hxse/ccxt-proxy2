"""实际取消 lifespan，确认后台初始化和资源清理不会脱离生命周期。"""

import asyncio
import threading

import pytest
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool

from src.tools.ctp_manager import CtpManager
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan
from src.tools.tq_manager import TqManager
from Test.ctp_fakes import FakeFactory
from Test.test_service_lifecycle import configured, fake_managers
from Test.test_tq_lifecycle import OwnedApi


async def wait_for_event(event):
    async with asyncio.timeout(3):
        while not event.is_set():
            await asyncio.sleep(0.001)


def install_managers(monkeypatch, runtime, ccxt, tq, ctp, events):
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", ccxt)
    monkeypatch.setattr("src.tools.tq_manager.tq_manager", tq)
    monkeypatch.setattr("src.tools.ctp_manager.ctp_manager", ctp)
    monkeypatch.setattr(
        "src.tools.telegram_manager.telegram_manager.close",
        lambda: events.append("close:telegram"),
    )


@pytest.mark.parametrize("cancel_count", [1, 2])
def test_cancelled_lifespan_waits_for_active_init_without_publishing_it(
    tmp_path, monkeypatch, cancel_count
):
    config = configured(tmp_path)
    runtime = ServiceRuntime(config)
    events = []
    ccxt, _, _ = fake_managers(events)
    tq = TqManager(config.tq, lock_path=tmp_path / "tq.lock")
    factory = FakeFactory()
    ctp = CtpManager(config.ctp, factory)
    entered, release, accepted = (threading.Event() for _ in range(3))
    created = []

    def create_api(config):
        entered.set()
        assert release.wait(3)
        api = OwnedApi()
        created.append(api)
        return api

    monkeypatch.setattr(tq._client, "_create_api", create_api)
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def serve():
        async with lifespan(FastAPI()):
            accepted.set()
            await asyncio.Event().wait()

    async def run():
        task = asyncio.create_task(serve())
        try:
            await wait_for_event(entered)
            await wait_for_event(accepted)
            assert runtime.ready
            for _ in range(cancel_count):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            assert not runtime.ready
            assert "close:ccxt" not in events
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=3)

    try:
        asyncio.run(run())
        assert ctp._clients == {}
        assert len(created) == 1 and created[0].closed.is_set()
        assert tq._worker._thread is None
        assert not runtime.ready and runtime.initialized == []
        assert events[-2:] == ["close:ccxt", "close:telegram"]
    finally:
        runtime.close()
        tq.close()
        ctp.close()


def test_repeated_cancellation_during_shutdown_waits_for_all_closers(
    tmp_path, monkeypatch
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    ccxt, tq, ctp = fake_managers(events)
    entered, release = threading.Event(), threading.Event()
    original_close = ctp.close

    def close():
        entered.set()
        assert release.wait(3)
        original_close()

    ctp.close = close
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def serve():
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            assert runtime.ready

    async def run():
        task = asyncio.create_task(serve())
        try:
            await wait_for_event(entered)
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=3)

    asyncio.run(run())
    assert sorted(events[:3]) == ["init:ccxt", "init:ctp", "init:tq"]
    assert events[3:] == ["close:ctp", "close:tq", "close:ccxt", "close:telegram"]
    assert not runtime.ready and runtime.initialized == []


def test_cancelled_before_startup_thread_runs_never_initializes_services(
    tmp_path, monkeypatch
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    ccxt, tq, ctp = fake_managers(events)
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def run():
        entered, release = asyncio.Event(), asyncio.Event()

        async def delayed_threadpool(func, *args, **kwargs):
            if func == runtime.start:
                entered.set()
                await release.wait()
            return await run_in_threadpool(func, *args, **kwargs)

        monkeypatch.setattr("src.tools.shared.run_in_threadpool", delayed_threadpool)

        async def serve():
            async with lifespan(FastAPI()):
                pytest.fail("cancelled startup accepted HTTP requests")

        task = asyncio.create_task(serve())
        try:
            await asyncio.wait_for(entered.wait(), timeout=3)
            task.cancel()
            await asyncio.sleep(0)
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=3)

    asyncio.run(run())
    assert events == ["close:telegram"]
    assert not runtime.ready and runtime.initialized == []


@pytest.mark.parametrize("fail", ["ccxt", "tq", "ctp"])
def test_lifespan_isolates_sdk_failures_and_cleans_all_owned_resources(
    tmp_path, monkeypatch, fail
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    ccxt, tq, ctp = fake_managers(events, fail)
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def run():
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            assert runtime.ready
            assert list(runtime.snapshot()["services"].values()).count("failed") == 1

    asyncio.run(run())
    initialized = [
        event.removeprefix("init:") for event in events if event.startswith("init:")
    ]
    assert sorted(initialized) == ["ccxt", "ctp", "tq"]
    assert events[len(initialized) :] == [
        "close:ctp",
        "close:tq",
        "close:ccxt",
        "close:telegram",
    ]
    assert not runtime.ready and runtime.initialized == []
