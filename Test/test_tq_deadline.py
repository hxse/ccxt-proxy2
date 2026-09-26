import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import HTTPException

from src.tools.tq_worker import TqWorker


def test_expired_queued_task_never_runs_after_busy_task_finishes():
    entered, release = threading.Event(), threading.Event()
    executed = []

    class Client:
        def initialize(self):
            pass

        def pump(self, *, wait=True):
            time.sleep(0.001)

        def close(self):
            pass

    worker = TqWorker(Client())
    worker.start()

    def busy():
        entered.set()
        assert release.wait(2)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            first = pool.submit(worker.call, busy)
            assert entered.wait(1)
            try:
                with pytest.raises(HTTPException, match="TQ_DATA_TIMEOUT"):
                    worker.call(
                        lambda: executed.append(True), deadline=time.monotonic() + 0.02
                    )
            finally:
                release.set()
            first.result(timeout=1)
        assert worker.call(lambda: "done", deadline=time.monotonic() + 1) == "done"
        assert executed == []
    finally:
        worker.close()
