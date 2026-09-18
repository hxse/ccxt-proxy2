"""TQ 的单一 SDK 线程：启动、请求、消息循环与关闭都在同一线程。"""

import threading
from collections.abc import Callable
from concurrent.futures import Future
from queue import Empty, Queue
from typing import Any

from fastapi import HTTPException
from loguru import logger


class TqWorker:
    def __init__(self, client: Any):
        self.client = client
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._ready: Future[None] = Future()
        self._queue: Queue[tuple[Callable[[], Any], Future[Any]]] = Queue()
        self._accepting = False

    def start(self) -> None:
        with self._lock:
            if self._thread is None:
                self._stop.clear()
                self._ready = Future()
                self._thread = threading.Thread(
                    target=self._run, name="tq-sdk", daemon=True
                )
                self._thread.start()
        self._ready.result()

    def call[T](self, operation: Callable[[], T]) -> T:
        result: Future[T] = Future()
        with self._lock:
            if not self._accepting or self._stop.is_set():
                raise HTTPException(503, detail="TQ_NOT_READY")
            self._queue.put((operation, result))
        return result.result()

    def close(self) -> None:
        with self._lock:
            self._stop.set()
            self._accepting = False
            thread = self._thread
        if thread is not None:
            thread.join()
        with self._lock:
            if self._thread is thread:
                self._thread = None

    def _run(self) -> None:
        try:
            self.client.initialize()
            with self._lock:
                self._accepting = not self._stop.is_set()
            self._ready.set_result(None)
            while not self._stop.is_set():
                try:
                    operation, result = self._queue.get_nowait()
                except Empty:
                    self.client.pump()
                    continue
                if result.set_running_or_notify_cancel():
                    try:
                        result.set_result(operation())
                    except BaseException as exc:
                        result.set_exception(exc)
                if not self._stop.is_set():
                    # 队列持续有任务时也处理订阅和已到达的消息，不额外等网络。
                    self.client.pump(wait=False)
        except BaseException as exc:
            if not self._ready.done():
                self._ready.set_exception(exc)
            else:
                logger.bind(error_type=type(exc).__name__).error(
                    "TQ message loop stopped"
                )
        finally:
            with self._lock:
                self._accepting = False
                self._stop.set()
                while True:
                    try:
                        _, pending = self._queue.get_nowait()
                    except Empty:
                        break
                    pending.set_exception(HTTPException(503, detail="TQ_NOT_READY"))
            try:
                self.client.close()
            except Exception:
                logger.error("TQ shutdown failed")
