"""TQ SDK 操作复用启动线程；交易状态独立读快照，请求不触发初始化。"""

import asyncio
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from threading import Condition
from time import monotonic
from typing import Any

from fastapi import HTTPException

from src.cache_tool import DuckDbOhlcvCache
from src.responses_tq import (
    TqTradingStatusResponse,
    TqUnderlyingSymbolResponse,
)
from src.tools import tq_ohlcv
from src.tools.config_types import TqConfig
from src.tools.shared import config, service_runtime
from src.tools.tq_client import TQ_HTTP_UPDATE_TIMEOUT_SECONDS, TqClient
from src.tools.tq_metadata_query import TqMetadataQuery
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
        self._cache: DuckDbOhlcvCache | None = None
        self._lifecycle = Condition()
        self._active = 0
        self._closing = False
        self._metadata: TqMetadataQuery | None = None
        self._metadata_tasks: set[asyncio.Task] = set()

    def initialize(self, cache: DuckDbOhlcvCache | None = None) -> None:
        self._cache = cache
        self._closing = False
        self._worker.start()

    def _call[T](self, operation: Callable[[], T]) -> T:
        if self._access_guard is not None:
            self._access_guard("tq")
        return self._worker.call(operation)

    async def fetch_ohlcv(self, request: TqOhlcvRequest) -> list[dict[str, Any]]:
        with self._business_scope():
            try:
                records = await asyncio.to_thread(self.fetch_raw_ohlcv, request)
            except HTTPException as exc:
                if (
                    not tq_ohlcv.is_network_failure(exc)
                    or not request.enable_cache
                    or request.duration_seconds > 604800
                ):
                    raise
                return await self._closed_market_fallback(request, exc)
            if self._worker.stop_event.is_set():
                raise HTTPException(503, detail="TQ_NOT_READY")
            return await asyncio.to_thread(
                tq_ohlcv.cache_result, request, records, self._cache
            )

    def fetch_raw_ohlcv(
        self, request: TqOhlcvRequest, *, deadline: float | None = None
    ) -> list[dict[str, Any]]:
        if self._access_guard is not None:
            self._access_guard("tq")
        end = (
            min(deadline, monotonic() + 10)
            if deadline is not None
            else monotonic() + 10
        )
        return self._worker.call(
            lambda: self._client.fetch_ohlcv(
                request, deadline=end, stop=self._worker.stop_event
            ),
            deadline=end,
        )

    async def _closed_market_fallback(
        self, request: TqOhlcvRequest, original: HTTPException
    ):
        symbol, resolve = tq_ohlcv.status_symbol(request.symbol)
        if resolve:
            try:
                mapping = await self.fetch_underlying_symbol(
                    TqUnderlyingSymbolRequest(symbol=symbol)
                )
                symbol = tq_ohlcv.require_actual_symbol(
                    mapping.items[0].underlying_symbol
                )
            except HTTPException as exc:
                if exc.status_code == 403:
                    raise
                raise HTTPException(
                    502, detail="TQ_TRADING_STATUS_UNAVAILABLE"
                ) from exc
        status = self._client.status_snapshot.read(symbol)
        if status.raw_status == "NOTRADING" and status.is_open is False:
            return await asyncio.to_thread(
                tq_ohlcv.closed_market_result, request, self._cache
            )
        if status.is_open is True or status.raw_status == "AUCTIONORDERING":
            raise original
        raise HTTPException(502, detail="TQ_TRADING_STATUS_UNAVAILABLE")

    @contextmanager
    def _business_scope(self):
        with self._lifecycle:
            if self._closing:
                raise HTTPException(503, detail="TQ_NOT_READY")
            self._active += 1
        try:
            yield
        finally:
            with self._lifecycle:
                self._active -= 1
                self._lifecycle.notify_all()

    def fetch_tick(self, request: TqTickRequest) -> list[dict[str, Any]]:
        return self._call(lambda: self._client.fetch_tick(request))

    def _metadata_query(self) -> TqMetadataQuery:
        if self._access_guard is not None:
            self._access_guard("tq")
        if self._closing:
            raise HTTPException(503, "TQ_NOT_READY")
        if self._metadata is None:
            self._metadata = TqMetadataQuery(
                self._cache,
                self._metadata_headers,
                self._mapping_reference,
                self._transition_raw,
            )
        return self._metadata

    async def _transition_raw(self, request, deadline):
        return await asyncio.to_thread(self.fetch_raw_ohlcv, request, deadline=deadline)

    async def _metadata_headers(self):
        deadline = monotonic() + 10
        return await asyncio.to_thread(
            self._worker.call, self._client.metadata_headers, deadline=deadline
        )

    async def _mapping_reference(self, symbol):
        deadline = monotonic() + 10
        return await asyncio.to_thread(
            self._worker.call,
            lambda: self._client.fetch_mapping_reference_time(
                symbol, deadline, self._worker.stop_event
            ),
            deadline=deadline,
        )

    async def fetch_underlying_symbol(
        self, request: TqUnderlyingSymbolRequest
    ) -> TqUnderlyingSymbolResponse:
        query = self._metadata_query()
        task = asyncio.current_task()
        assert task is not None
        self._metadata_tasks.add(task)
        try:
            with self._business_scope():
                return await query.mapping(request)
        finally:
            self._metadata_tasks.discard(task)

    async def fetch_trading_calendar(
        self, request: TqTradingCalendarRequest
    ) -> list[dict[str, object]]:
        query = self._metadata_query()
        task = asyncio.current_task()
        assert task is not None
        self._metadata_tasks.add(task)
        try:
            with self._business_scope():
                return await query.calendar(request)
        finally:
            self._metadata_tasks.discard(task)

    async def close_metadata(self):
        self._closing = True
        tasks = list(self._metadata_tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._metadata is not None:
            await self._metadata.close()
            self._metadata = None

    def fetch_trading_status(
        self, request: TqTradingStatusRequest
    ) -> TqTradingStatusResponse:
        if self._access_guard is not None:
            self._access_guard("tq")
        return self._client.status_snapshot.read(request.symbol)

    def close(self) -> None:
        with self._lifecycle:
            self._closing = True
        self._client.status_snapshot.deactivate()
        self._worker.close()
        with self._lifecycle:
            self._lifecycle.wait_for(lambda: self._active == 0)


tq_manager = TqManager(config.tq, access_guard=service_runtime.require)
