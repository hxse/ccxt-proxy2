import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx
from anyio.to_thread import current_default_thread_limiter
from fastapi import FastAPI
from fastapi.concurrency import run_in_threadpool

from src.router.auth_handler import manager as auth_manager
from src.router.ctp_router import ctp_router
from src.router.tq_router import tq_router
from src.tools.config_types import TqConfig, UserConfig
from src.tools.ctp_manager import CtpManager
from src.tools.tq_manager import TqManager
from src.types_ctp import CtpTradingStatusQuery
from src.types_tq import TqTickRequest, TqTradingStatusRequest
from Test.ctp_fakes import FakeFactory, ctp_config
from Test.test_ctp_trading_status import status_record
from Test.test_tq_lifecycle import OwnedApi
from Test.test_tq_trading_status import SYMBOL, notice, status


def test_tq_status_returns_while_sdk_query_is_blocked(tmp_path, monkeypatch):
    manager = TqManager(TqConfig(), lock_path=tmp_path / "tq.lock")
    entered, release = threading.Event(), threading.Event()

    class SlowApi(OwnedApi):
        def get_tick_serial(self, *args, **kwargs):
            entered.set()
            assert release.wait(5)
            return super().get_tick_serial(*args, **kwargs)

    monkeypatch.setattr(manager._client, "_create_api", lambda config: SlowApi())
    manager.initialize()
    manager._client.status_snapshot.observe([notice(2019112901), status()])
    with ThreadPoolExecutor(max_workers=3) as pool:
        try:
            slow = pool.submit(manager.fetch_tick, TqTickRequest(symbol=SYMBOL))
            assert entered.wait(2)
            snapshot = pool.submit(
                manager.fetch_trading_status, TqTradingStatusRequest(symbol=SYMBOL)
            )
            assert snapshot.result(timeout=1).is_open is True
            cold = pool.submit(
                manager.fetch_trading_status,
                TqTradingStatusRequest(symbol="SHFE.cu2610"),
            )
            assert cold.result(timeout=1).reason == "not_received"
            assert not slow.done()
        finally:
            close = pool.submit(manager.close)
            assert manager._worker._stop.wait(2)
            release.set()
            close.result(timeout=2)
        assert slow.result(timeout=2)


def test_ctp_snapshot_ignores_business_lock_and_disconnect_invalidates_before_callback_lock(
    tmp_path,
):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    manager.initialize("sandbox")
    client = manager.get_client("sandbox")
    callbacks = factory.apis[0].callbacks
    callbacks.on_instrument_status(status_record())
    request = CtpTradingStatusQuery(exchange_id="SHFE", product_id="rb")
    entered, release = threading.Event(), threading.Event()

    def locked():
        with manager._lock, client._lock, callbacks.lock:
            entered.set()
            assert release.wait(5)

    with ThreadPoolExecutor(max_workers=3) as pool:
        holder = pool.submit(locked)
        try:
            assert entered.wait(2)
            assert (
                pool.submit(manager.fetch_trading_status, request)
                .result(timeout=1)
                .is_open
                is True
            )
            disconnected = threading.Event()
            original = callbacks.status_snapshot.disconnect

            def invalidate(**kwargs):
                original(**kwargs)
                disconnected.set()

            callbacks.status_snapshot.disconnect = invalidate
            callback = pool.submit(callbacks.on_disconnected)
            assert disconnected.wait(1)
            result = pool.submit(manager.fetch_trading_status, request).result(
                timeout=1
            )
            assert result.is_open is None and result.reason == "disconnected"
            assert not callback.done()  # 原有业务回调锁仍未释放。
        finally:
            release.set()
            holder.result(timeout=2)
    manager.close()


def test_status_http_and_auth_do_not_wait_for_exhausted_sync_thread_pool(
    tmp_path, monkeypatch
):
    tq = TqManager(TqConfig(), lock_path=tmp_path / "tq.lock")
    tq._client.status_snapshot.activate(True)
    tq._client.status_snapshot.observe([notice(2019112901), status()])
    factory = FakeFactory()
    ctp = CtpManager(ctp_config(tmp_path), factory)
    ctp.initialize("sandbox")
    factory.apis[0].callbacks.on_instrument_status(status_record())
    monkeypatch.setattr("src.router.tq_router.tq_manager", tq)
    monkeypatch.setattr("src.router.ctp_router.ctp_manager", ctp)
    monkeypatch.setattr(
        "src.router.auth_handler.config.users", {"offline": UserConfig(password="test")}
    )
    token = auth_manager.create_access_token(data={"sub": "offline"})
    app = FastAPI()
    app.include_router(tq_router)
    app.include_router(ctp_router)
    entered, release = threading.Event(), threading.Event()

    def consume_thread():
        entered.set()
        assert release.wait(5)

    async def run():
        limiter = current_default_thread_limiter()
        previous = limiter.total_tokens
        limiter.total_tokens = 1
        busy = asyncio.create_task(run_in_threadpool(consume_thread))
        try:
            while not entered.is_set():
                await asyncio.sleep(0.001)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://test"
            ) as http:
                for route, params in [
                    ("tq", {"symbol": SYMBOL}),
                    ("ctp", {"exchange_id": "SHFE", "product_id": "rb"}),
                ]:
                    response = await asyncio.wait_for(
                        http.get(
                            f"/{route}/fetch_trading_status",
                            params=params,
                            headers={"Authorization": "Bearer " + token},
                        ),
                        timeout=1,
                    )
                    assert (
                        response.status_code == 200
                        and response.json()["is_open"] is True
                    )
            assert not busy.done()
        finally:
            release.set()
            await busy
            limiter.total_tokens = previous

    try:
        asyncio.run(run())
    finally:
        ctp.close()
