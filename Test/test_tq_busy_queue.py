"""排队业务不能饿死状态订阅和通知；所有输入均为离线数据。"""

import time
from concurrent.futures import Future

import pytest

from src.tools.tq_status_snapshot import TqStatusSnapshot
from src.tools.tq_worker import TqWorker

SYMBOL = "SHFE.rb2610"
CONNECTED = {"notify": {"connected": {"conn_id": "ts", "code": 2019112901}}}
DISCONNECTED = {"notify": {"disconnected": {"conn_id": "ts", "code": 2019112911}}}


def status(value):
    return {"trading_status": {SYMBOL: {"trade_status": value}}}


@pytest.mark.parametrize("change", ["subscribe", "closed", "disconnected"])
@pytest.mark.parametrize("operation_fails", [False, True])
def test_status_advances_between_queued_jobs(change, operation_fails):
    snapshot = TqStatusSnapshot()
    subscriptions = []
    pending_messages = []
    pump_modes = []

    class Client:
        def initialize(self):
            snapshot.activate(True)
            if change != "subscribe":
                snapshot.read(SYMBOL)
                snapshot.take_pending()
                snapshot.observe([CONNECTED, status("CONTINOUS")])

        def pump(self, *, wait=True):
            pump_modes.append(wait)
            for symbol in snapshot.take_pending():
                subscriptions.append(symbol)
                pending_messages.extend([CONNECTED, status("CONTINOUS")])
            snapshot.observe(pending_messages)
            pending_messages.clear()
            if wait:
                time.sleep(0.005)

        def close(self):
            snapshot.deactivate()

    def first_job():
        if change == "subscribe":
            assert snapshot.read(SYMBOL).reason == "not_received"
        elif change == "closed":
            pending_messages.append(status("NOTRADING"))
        else:
            pending_messages.append(DISCONNECTED)
        if operation_fails:
            raise RuntimeError("offline business error")

    worker = TqWorker(Client())
    first, observed = Future(), Future()
    # 预先填入两个任务，确保从第一个完成到第二个读取时队列始终非空。
    worker._queue.put((first_job, first))
    worker._queue.put((lambda: snapshot.read(SYMBOL), observed))
    try:
        worker.start()
        if operation_fails:
            with pytest.raises(RuntimeError, match="offline business error"):
                first.result(timeout=2)
        else:
            first.result(timeout=2)
        result = observed.result(timeout=2)
        if change == "subscribe":
            assert result.is_open is True
            assert subscriptions == [SYMBOL]
        elif change == "closed":
            assert result.is_open is False and result.raw_status == "NOTRADING"
        else:
            assert result.is_open is None and result.reason == "disconnected"
        assert pump_modes[0] is False
    finally:
        worker.close()
