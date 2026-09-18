import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from src.tools.config_types import TqConfig
from src.tools.tq_manager import TqManager
from src.tools.tq_worker import TqWorker
from src.types_tq import TqTickRequest, TqTradingStatusRequest
from Test.test_tq_data_source import FakeTqApi


class OwnedApi(FakeTqApi):
    def __init__(self):
        super().__init__()
        self.owner = threading.get_ident()
        self.pumped = threading.Event()
        self.closed = threading.Event()
        self.fail_pump = False

    def has_trading_status_permission(self):
        return True

    def wait_update(self, deadline=None):
        assert threading.get_ident() == self.owner
        self.pumped.set()
        if self.fail_pump:
            raise RuntimeError("offline loop failure")
        time.sleep(0.005)
        return False

    def get_tick_serial(self, *args, **kwargs):
        assert threading.get_ident() == self.owner
        return super().get_tick_serial(*args, **kwargs)

    def close(self):
        assert threading.get_ident() == self.owner
        self.closed.set()


def test_tq_initializes_before_requests_pumps_while_idle_and_owns_one_thread(
    tmp_path, monkeypatch
):
    config = TqConfig(username="original", password="original")
    manager = TqManager(config, lock_path=tmp_path / "tq.lock")
    created = []

    def create(snapshot):
        assert snapshot.password == "original"
        api = OwnedApi()
        created.append(api)
        return api

    monkeypatch.setattr(manager._client, "_create_api", create)
    with pytest.raises(HTTPException, match="TQ_NOT_READY"):
        manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2610"))
    assert created == []
    config.password = "edited"
    try:
        manager.initialize()
        manager.initialize()
        assert len(created) == 1 and created[0].owner != threading.get_ident()
        assert created[0].pumped.wait(2)  # 没有任何 HTTP 请求也推进消息循环。
        with ThreadPoolExecutor(max_workers=3) as pool:
            responses = list(
                pool.map(
                    lambda _: manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2610")),
                    range(3),
                )
            )
        assert all(rows[0]["last_price"] == 3605.0 for rows in responses)
        assert len(created) == 1
    finally:
        manager.close()
    assert created[0].closed.is_set()
    with pytest.raises(HTTPException, match="TQ_NOT_READY"):
        manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2610"))
    assert len(created) == 1


def test_tq_startup_failure_does_not_leave_a_thread_or_retry_on_request(
    tmp_path, monkeypatch
):
    manager = TqManager(TqConfig(), lock_path=tmp_path / "tq.lock")
    attempts = []

    def fail(config):
        attempts.append(1)
        raise HTTPException(502, "TQ_NETWORK_UNAVAILABLE")

    monkeypatch.setattr(manager._client, "_create_api", fail)
    try:
        with pytest.raises(HTTPException, match="TQ_NETWORK_UNAVAILABLE"):
            manager.initialize()
    finally:
        manager.close()
    with pytest.raises(HTTPException, match="TQ_NOT_READY"):
        manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2610"))
    assert attempts == [1] and manager._worker._thread is None


def test_background_loop_failure_invalidates_access_and_closes_on_owner_thread(
    tmp_path, monkeypatch
):
    manager = TqManager(TqConfig(), lock_path=tmp_path / "tq.lock")
    created = []

    def create(config):
        api = OwnedApi()
        created.append(api)
        return api

    monkeypatch.setattr(manager._client, "_create_api", create)
    try:
        manager.initialize()
        created[0].fail_pump = True
        assert created[0].closed.wait(2)
        with pytest.raises(HTTPException, match="TQ_NOT_READY"):
            manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2610"))
        result = manager.fetch_trading_status(
            TqTradingStatusRequest(symbol="SHFE.rb2610")
        )
        assert result.is_open is None and result.reason == "unavailable"
    finally:
        manager.close()


def test_shutdown_waits_for_active_tq_call_and_rejects_queued_work():
    entered, release, stopped = threading.Event(), threading.Event(), threading.Event()

    class Client:
        def initialize(self):
            pass

        def pump(self, *, wait=True):
            time.sleep(0.005)

        def close(self):
            stopped.set()

    worker = TqWorker(Client())
    worker.start()

    def active():
        entered.set()
        assert release.wait(3)
        return "done"

    with ThreadPoolExecutor(max_workers=3) as pool:
        first = pool.submit(worker.call, active)
        assert entered.wait(2)
        queued = pool.submit(
            worker.call, lambda: pytest.fail("queued work ran during shutdown")
        )
        close = pool.submit(worker.close)
        assert worker._stop.wait(2) and not stopped.is_set()
        release.set()
        assert first.result(timeout=2) == "done"
        with pytest.raises(HTTPException, match="TQ_NOT_READY"):
            queued.result(timeout=2)
        close.result(timeout=2)
    assert stopped.is_set()
