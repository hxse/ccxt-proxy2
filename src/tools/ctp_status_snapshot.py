"""CTP 品种状态的独立短锁；读取不进入账户请求锁或回调关联锁。"""

import threading
from typing import Any


class CtpStatusSnapshot:
    def __init__(self):
        self._lock = threading.Lock()
        self._connected: bool | None = None
        self._closed = False
        self._records: dict[tuple[str, str], dict[str, Any]] = {}

    def connect(self) -> None:
        with self._lock:
            if not self._closed:
                self._connected = True

    def disconnect(self, *, closed: bool = False) -> None:
        with self._lock:
            self._connected = False
            self._closed = self._closed or closed
            self._records.clear()

    def update(self, record: dict[str, Any]) -> None:
        with self._lock:
            if self._closed or not self._connected:
                return
            exchange, product = record.get("ExchangeID"), record.get("InstrumentID")
            if exchange and product:
                self._records[(exchange, product)] = dict(record)

    def read(
        self, exchange: str, product: str
    ) -> tuple[bool | None, dict[str, Any] | None]:
        with self._lock:
            record = self._records.get((exchange, product))
            return self._connected, dict(record) if record is not None else None
