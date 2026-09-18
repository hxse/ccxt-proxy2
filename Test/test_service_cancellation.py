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
def test_cancelled_startup_finishes_active_init_closes_it_and_skips_later_services(
    tmp_path, monkeypatch, cancel_count
):
    config = configured(tmp_path)
    runtime = ServiceRuntime(config)
    events = []
    ccxt, _, _ = fake_managers(events)
    tq = TqManager(config.tq, lock_path=tmp_path / "tq.lock")
    factory = FakeFactory()
    ctp = CtpManager(config.ctp, factory)
    entered, release, finished = (threading.Event() for _ in range(3))
    created = []

    def create_api(config):
        entered.set()
        assert release.wait(3)
        api = OwnedApi()
        created.append(api)
        return api

    original_start = runtime.start

    def start(*args, **kwargs):
        try:
            original_start(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(tq._client, "_create_api", create_api)
    monkeypatch.setattr(runtime, "start", start)
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def serve():
        async with lifespan(FastAPI()):
            pytest.fail("cancelled startup accepted HTTP requests")

    async def run():
        task = asyncio.create_task(serve())
        try:
            await wait_for_event(entered)
            for _ in range(cancel_count):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
            assert "close:ccxt" not in events
        finally:
            release.set()
            try:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=3)
            finally:
                await wait_for_event(finished)

    try:
        asyncio.run(run())
        assert factory.apis == []  # 取消发生于 TQ 初始化，不能再启动后面的 CTP。
        assert len(created) == 1 and created[0].closed.is_set()
        assert tq._worker._thread is None
        assert not runtime.ready and runtime.initialized == []
        assert events == ["init:ccxt", "close:ccxt", "close:telegram"]
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
    assert events == [
        "init:ccxt",
        "init:tq",
        "init:ctp",
        "close:ctp",
        "close:tq",
        "close:ccxt",
        "close:telegram",
    ]
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
def test_lifespan_preserves_startup_errors_and_cleans_partial_resources(
    tmp_path, monkeypatch, fail
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    ccxt, tq, ctp = fake_managers(events, fail)
    install_managers(monkeypatch, runtime, ccxt, tq, ctp, events)

    async def run():
        async with lifespan(FastAPI()):
            pytest.fail("failed startup accepted HTTP requests")

    with pytest.raises(RuntimeError, match="offline initialization failed"):
        asyncio.run(run())
    initialized = [
        event.removeprefix("init:") for event in events if event.startswith("init:")
    ]
    assert events[len(initialized) :] == [
        *("close:" + name for name in reversed(initialized)),
        "close:telegram",
    ]
    assert not runtime.ready and runtime.initialized == []
