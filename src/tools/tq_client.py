import time
from contextlib import contextmanager
from pathlib import Path
from threading import Event
from typing import Any

from fastapi import HTTPException
from filelock import FileLock, Timeout
from loguru import logger

from src.tools import tq_data_source, tq_metadata_sdk
from src.tools.config_types import TqConfig
from src.tools.tq_errors import TqLegacyMetadataCallForbidden
from src.tools.tq_ohlcv_validation import validate_records
from src.tools.tq_serial import get_serial, require_budget
from src.tools.tq_status_snapshot import TqStatusSnapshot
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
)

TQ_HTTP_UPDATE_TIMEOUT_SECONDS = 0.2


class TqClient:
    def __init__(
        self,
        tq_config: TqConfig | None,
        lock_path: Path | None = None,
        update_timeout_seconds: float = TQ_HTTP_UPDATE_TIMEOUT_SECONDS,
    ):
        self._config = tq_config
        self._api: Any | None = None
        self.status_snapshot = TqStatusSnapshot()
        self._update_timeout_seconds = update_timeout_seconds
        self._lock_path = lock_path or Path("./data/tq/tqapi.lock")
        self._lock = FileLock(self._lock_path)

    def initialize(self) -> None:
        if self._config is None:
            raise HTTPException(500, detail="TQ_NOT_CONFIGURED")
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self._api is None:
                self._api = self._create_api(self._config)
                self.status_snapshot.activate(self._api.has_trading_status_permission())

    def pump(self, *, wait: bool = True) -> None:
        with self._lock:
            api = self._get_api()
            for symbol in self.status_snapshot.take_pending():
                api.create_task(self._subscribe_trading_status(api, symbol))
            timeout = self._update_timeout_seconds if wait else 0
            api.wait_update(deadline=time.time() + timeout)

    async def _subscribe_trading_status(self, api: Any, symbol: str) -> None:
        # 在 SDK 协程中调用，只登记订阅，不进入其同步的 30 秒等待分支。
        try:
            api.get_trading_status(symbol)
        except Exception as exc:
            error = (
                HTTPException(403, detail="TQ_TRADING_STATUS_PERMISSION_DENIED")
                if "账户不支持查看交易状态" in str(exc)
                else self._map_tq_exception(exc)
            )
            self.status_snapshot.subscription_failed(
                symbol, error.status_code, str(error.detail)
            )

    def fetch_ohlcv(
        self,
        request: TqOhlcvRequest,
        *,
        deadline: float | None = None,
        stop: Event | None = None,
    ) -> list[dict[str, Any]]:
        end = deadline if deadline is not None else time.monotonic() + 10
        with self._bounded_lock(end, stop):
            api = self._get_api()
            try:
                frame = get_serial(
                    api,
                    request,
                    end,
                    stop,
                )
                records = tq_data_source.clean_tq_serial_records(frame, "kline")
                validate_records(records, request.symbol, request.duration_seconds)
                return records
            except tq_data_source.TqDataFrameError as exc:
                raise HTTPException(status_code=422, detail=exc.detail) from exc
            except HTTPException:
                raise
            except Exception as exc:
                raise self._map_tq_exception(exc, ohlcv=True) from exc

    @contextmanager
    def _bounded_lock(self, deadline: float, stop: Event | None):
        try:
            with self._lock.acquire(timeout=require_budget(deadline, stop)):
                yield
        except Timeout as exc:
            raise HTTPException(504, detail="TQ_DATA_TIMEOUT") from exc

    def fetch_tick(self, request: TqTickRequest) -> list[dict[str, Any]]:
        with self._lock:
            api = self._get_api()
            try:
                frame = api.get_tick_serial(
                    request.symbol,
                    request.data_length,
                    adj_type=request.adj_type,
                )
                self._wait_update_once(api)
                return tq_data_source.clean_tq_serial_records(frame, "tick")
            except tq_data_source.TqDataFrameError as exc:
                raise HTTPException(status_code=422, detail=exc.detail) from exc
            except HTTPException:
                raise
            except Exception as exc:
                raise self._map_tq_exception(exc) from exc

    def metadata_headers(self) -> dict[str, str]:
        with self._lock:
            return dict(self._get_api()._base_headers)

    def fetch_mapping_reference_time(self, symbol: str, deadline: float, stop: Event):
        with self._bounded_lock(deadline, stop):
            try:
                return tq_metadata_sdk.reference_time(
                    self._get_api(), symbol, deadline, stop
                )
            except HTTPException:
                raise
            except Exception as exc:
                raise self._map_tq_exception(exc) from exc

    def close(self) -> None:
        self.status_snapshot.deactivate()
        with self._lock:
            if self._api is None:
                return
            self._api.close()
            self._api = None

    def _get_api(self) -> Any:
        if self._config is None:
            raise HTTPException(status_code=500, detail="TQ_NOT_CONFIGURED")
        if self._api is None:
            raise HTTPException(503, detail="TQ_NOT_READY")
        return self._api

    def _wait_update_once(self, api: Any) -> None:
        deadline = time.time() + self._update_timeout_seconds
        api.wait_update(deadline=deadline)

    def _create_api(self, tq_config: TqConfig) -> Any:
        Path("./data/tq").mkdir(parents=True, exist_ok=True)
        try:
            from tqsdk import TqAuth

            from src.tools.tq_trading_status import TradingStatusTqApi
        except ImportError as exc:
            raise HTTPException(status_code=500, detail="TQ_NOT_CONFIGURED") from exc

        try:
            auth = (
                TqAuth(tq_config.username, tq_config.password)
                if tq_config.username
                else None
            )
            return TradingStatusTqApi(
                auth=auth, disable_print=True, status_snapshot=self.status_snapshot
            )
        except Exception as exc:
            raise self._map_tq_exception(exc) from exc

    def _map_tq_exception(self, exc: Exception, *, ohlcv=False) -> HTTPException:
        if isinstance(exc, TqLegacyMetadataCallForbidden):
            return HTTPException(500, detail="TQ_LEGACY_METADATA_CALL_FORBIDDEN")
        message = str(exc)
        logger.bind(error_type=type(exc).__name__).warning(
            "TQ call failed: {}", message
        )
        if any(
            text in message
            for text in (
                "权限",
                "账户不支持",
                "认证失败",
                "密码错误",
                "无权",
                "permission",
                "grants",
            )
        ):
            return HTTPException(403, detail="TQ_PERMISSION_DENIED")
        if "adj_type" in message or "复权" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_ADJ_TYPE")
        if "交易日历可以处理的范围为" in message:
            return HTTPException(
                status_code=422,
                detail="TQ_CALENDAR_RANGE_UNAVAILABLE",
            )
        if "K线数据周期" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_DURATION_SECONDS")
        if "序列长度" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_DATA_LENGTH")
        if "不能为空" in message or "参数错误" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_SYMBOL")
        if ohlcv:
            from requests.exceptions import ConnectionError as HttpConnectionError
            from requests.exceptions import Timeout as HttpTimeout
            from tqsdk.exceptions import TqTimeoutError
            from websockets.exceptions import ConnectionClosed

            if "代码" in message and "不存在" in message:
                return HTTPException(400, "TQ_INVALID_SYMBOL")
            if isinstance(exc, (TimeoutError, HttpTimeout, TqTimeoutError)):
                return HTTPException(504, "TQ_DATA_TIMEOUT")
            if not isinstance(
                exc, (ConnectionError, HttpConnectionError, ConnectionClosed)
            ):
                # 未知 SDK/内部错误不能伪装为断网，再用休市缓存掩盖。
                return HTTPException(502, "TQ_UPSTREAM_ERROR")
        return HTTPException(status_code=502, detail="TQ_NETWORK_UNAVAILABLE")
