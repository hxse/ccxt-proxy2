"""HTTP 只读 TQ 状态快照；新合约只登记订阅意向，SDK 线程稍后处理。"""

import threading
from typing import Any

from fastapi import HTTPException

from src.responses_tq import TqTradingStatusResponse


class TqStatusSnapshot:
    def __init__(self):
        self._lock = threading.Lock()
        self._available = False
        self._permitted = False
        self._connected: bool | None = None
        self._statuses: dict[str, str] = {}
        self._requested: set[str] = set()
        self._pending: list[str] = []
        self._errors: dict[str, tuple[int, str]] = {}

    def activate(self, permitted: bool) -> None:
        with self._lock:
            self._available, self._permitted = True, permitted
            self._connected = None
            self._statuses.clear()
            self._requested.clear()
            self._pending.clear()
            self._errors.clear()

    def deactivate(self) -> None:
        with self._lock:
            self._available = False
            self._statuses.clear()
            self._pending.clear()

    def read(self, symbol: str) -> TqTradingStatusResponse:
        with self._lock:
            result = TqTradingStatusResponse(symbol=symbol)
            if not self._available:
                result.reason = "unavailable"
                return result
            if not self._permitted:
                raise HTTPException(403, detail="TQ_TRADING_STATUS_PERMISSION_DENIED")
            if symbol in self._errors:
                status, detail = self._errors[symbol]
                raise HTTPException(status, detail=detail)
            if symbol not in self._requested:
                self._requested.add(symbol)
                self._pending.append(symbol)
            if self._connected is False:
                result.reason = "disconnected"
                return result
            result.raw_status = self._statuses.get(symbol) or None
        if result.raw_status is None:
            result.reason = "not_received"
        elif result.raw_status in {"CONTINOUS", "AUCTIONORDERING", "NOTRADING"}:
            result.is_open = result.raw_status == "CONTINOUS"
        else:
            result.reason = "unrecognized_status"
        return result

    def take_pending(self) -> list[str]:
        with self._lock:
            pending, self._pending = self._pending, []
            return pending

    def subscription_failed(self, symbol: str, status: int, detail: str) -> None:
        with self._lock:
            if self._available:
                self._statuses.pop(symbol, None)
                self._errors[symbol] = (status, detail)
                if status == 403:
                    self._permitted = False
                    self._statuses.clear()
                    self._pending.clear()

    def observe(self, diffs: list[dict[str, Any]]) -> None:
        # 只锁住本次快照更新，不持锁调用 SDK 或等待网络。
        with self._lock:
            if not self._available:
                return
            for diff in diffs:
                for notice in (diff.get("notify") or {}).values():
                    if not notice or notice.get("conn_id") != "ts":
                        continue
                    code = notice.get("code")
                    if code in {2019112901, 2019112902, 2019112910, 2019112911}:
                        self._connected = code in {2019112901, 2019112902}
                        self._statuses.clear()
                for symbol, status in (diff.get("trading_status") or {}).items():
                    if status is None:
                        self._statuses.pop(symbol, None)
                    elif self._connected and "trade_status" in status:
                        self._statuses[symbol] = status["trade_status"]
                        self._errors.pop(symbol, None)
