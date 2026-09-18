"""TQ SDK 操作复用启动线程；交易状态独立读快照，请求不触发初始化。"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.responses_tq import TqTradingStatusResponse, TqUnderlyingSymbolResponse
from src.tools.config_types import TqConfig
from src.tools.shared import config, service_runtime
from src.tools.tq_client import TQ_HTTP_UPDATE_TIMEOUT_SECONDS, TqClient
from src.tools.tq_worker import TqWorker
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
    TqTradingCalendarRequest,
    TqTradingStatusRequest,
    TqUnderlyingSymbolRequest,
)


class TqManager:
    def __init__(
        self,
        tq_config: TqConfig | None,
        lock_path: Path | None = None,
        update_timeout_seconds: float = TQ_HTTP_UPDATE_TIMEOUT_SECONDS,
        *,
        access_guard: Callable[[str], None] | None = None,
    ):
        snapshot = tq_config.model_copy(deep=True) if tq_config else None
        self._client = TqClient(snapshot, lock_path, update_timeout_seconds)
        self._worker = TqWorker(self._client)
        self._access_guard = access_guard

    def initialize(self) -> None:
        self._worker.start()

    def _call[T](self, operation: Callable[[], T]) -> T:
        if self._access_guard is not None:
            self._access_guard("tq")
        return self._worker.call(operation)

    def fetch_ohlcv(self, request: TqOhlcvRequest) -> list[dict[str, Any]]:
        return self._call(lambda: self._client.fetch_ohlcv(request))

    def fetch_tick(self, request: TqTickRequest) -> list[dict[str, Any]]:
        return self._call(lambda: self._client.fetch_tick(request))

    def fetch_underlying_symbol(
        self, request: TqUnderlyingSymbolRequest
    ) -> TqUnderlyingSymbolResponse:
        return self._call(lambda: self._client.fetch_underlying_symbol(request))

    def fetch_trading_calendar(
        self, request: TqTradingCalendarRequest
    ) -> list[dict[str, object]]:
        return self._call(lambda: self._client.fetch_trading_calendar(request))

    def fetch_trading_status(
        self, request: TqTradingStatusRequest
    ) -> TqTradingStatusResponse:
        if self._access_guard is not None:
            self._access_guard("tq")
        return self._client.status_snapshot.read(request.symbol)

    def close(self) -> None:
        self._client.status_snapshot.deactivate()
        self._worker.close()


tq_manager = TqManager(config.tq, access_guard=service_runtime.require)
