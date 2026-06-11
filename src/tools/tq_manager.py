from dataclasses import dataclass
from pathlib import Path
import time
from typing import Any

from fastapi import HTTPException
from filelock import FileLock
from loguru import logger

from src.responses_tq import (
    TqUnderlyingHistoryItem,
    TqUnderlyingItem,
    TqUnderlyingSymbolResponse,
)
from src.tools import tq_data_source
from src.tools.config_types import TqConfig
from src.tools.shared import config
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
    TqUnderlyingSymbolRequest,
)

TQ_HTTP_UPDATE_TIMEOUT_SECONDS = 0.2


@dataclass(frozen=True)
class _UnderlyingItemDraft:
    symbol: str
    underlying_symbol: str | None
    ins_class: str | None
    exchange_id: str | None
    product_id: str | None


class TqManager:
    def __init__(
        self,
        tq_config: TqConfig | None,
        lock_path: Path | None = None,
        update_timeout_seconds: float = TQ_HTTP_UPDATE_TIMEOUT_SECONDS,
    ):
        self._config = tq_config
        self._api: Any | None = None
        self._update_timeout_seconds = update_timeout_seconds
        self._lock_path = lock_path or Path("./data/tq/tqapi.lock")
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = FileLock(self._lock_path)

    def fetch_ohlcv(self, request: TqOhlcvRequest) -> list[dict[str, Any]]:
        with self._lock:
            api = self._get_api()
            try:
                frame = api.get_kline_serial(
                    request.symbol,
                    request.duration_seconds,
                    request.data_length,
                    adj_type=request.adj_type,
                )
                self._wait_update_once(api)
                return tq_data_source.clean_tq_serial_records(frame, "kline")
            except tq_data_source.TqDataFrameError as exc:
                raise HTTPException(status_code=422, detail=exc.detail) from exc
            except HTTPException:
                raise
            except Exception as exc:
                raise self._map_tq_exception(exc) from exc

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

    def fetch_underlying_symbol(
        self, request: TqUnderlyingSymbolRequest
    ) -> TqUnderlyingSymbolResponse:
        with self._lock:
            api = self._get_api()
            try:
                symbols = request.symbol_list
                items = self._query_underlying_items(api, symbols)
                history: list[TqUnderlyingHistoryItem] = []
                if request.n is not None:
                    history_frame = api.query_his_cont_quotes(
                        request.symbol, n=request.n
                    )
                    history = [
                        TqUnderlyingHistoryItem.model_validate(record)
                        for record in tq_data_source.history_wide_frame_to_records(
                            history_frame
                        )
                    ]
                return TqUnderlyingSymbolResponse(items=items, history=history)
            except tq_data_source.TqDataFrameError as exc:
                raise HTTPException(status_code=422, detail=exc.detail) from exc
            except HTTPException:
                raise
            except Exception as exc:
                raise self._map_tq_exception(exc) from exc

    def close(self) -> None:
        with self._lock:
            if self._api is None:
                return
            self._api.close()
            self._api = None

    def _get_api(self) -> Any:
        if self._config is None:
            raise HTTPException(status_code=500, detail="TQ_NOT_CONFIGURED")
        if self._api is None:
            self._api = self._create_api(self._config)
        return self._api

    def _wait_update_once(self, api: Any) -> None:
        deadline = time.time() + self._update_timeout_seconds
        api.wait_update(deadline=deadline)

    def _create_api(self, tq_config: TqConfig) -> Any:
        Path("./data/tq").mkdir(parents=True, exist_ok=True)
        try:
            from tqsdk import TqApi, TqAuth
        except ImportError as exc:
            raise HTTPException(status_code=500, detail="TQ_NOT_CONFIGURED") from exc

        try:
            auth = (
                TqAuth(tq_config.username, tq_config.password)
                if tq_config.username
                else None
            )
            return TqApi(auth=auth, disable_print=True)
        except Exception as exc:
            raise self._map_tq_exception(exc) from exc

    def _query_underlying_items(
        self, api: Any, symbols: list[str]
    ) -> list[TqUnderlyingItem]:
        info_frame = api.query_symbol_info(symbols[0] if len(symbols) == 1 else symbols)
        rows = tq_data_source.frame_rows(info_frame)
        items: list[TqUnderlyingItem] = []
        rows_by_symbol = {
            str(row.get("instrument_id")): row
            for row in rows
            if row.get("instrument_id") is not None
        }
        for index, symbol in enumerate(symbols):
            row = rows_by_symbol.get(symbol) or (
                rows[index] if index < len(rows) else {}
            )
            draft = self._underlying_item_from_row(symbol, row)
            if draft.underlying_symbol is None:
                quote = api.get_quote(symbol)
                api.wait_update(deadline=time.time() + 30)
                draft = self._underlying_item_from_quote(symbol, quote)
            items.append(self._validate_underlying_item(draft))
        return items

    def _underlying_item_from_row(
        self, symbol: str, row: dict[str, Any]
    ) -> _UnderlyingItemDraft:
        return _UnderlyingItemDraft(
            symbol=symbol,
            underlying_symbol=_clean_text(row.get("underlying_symbol")),
            ins_class=_clean_text(row.get("ins_class")),
            exchange_id=_clean_text(row.get("exchange_id")),
            product_id=_clean_text(row.get("product_id")),
        )

    def _underlying_item_from_quote(
        self, symbol: str, quote: Any
    ) -> _UnderlyingItemDraft:
        return _UnderlyingItemDraft(
            symbol=symbol,
            underlying_symbol=_clean_text(getattr(quote, "underlying_symbol", None)),
            ins_class=_clean_text(getattr(quote, "ins_class", None)),
            exchange_id=_clean_text(getattr(quote, "exchange_id", None)),
            product_id=_clean_text(getattr(quote, "product_id", None)),
        )

    def _validate_underlying_item(
        self, item: _UnderlyingItemDraft
    ) -> TqUnderlyingItem:
        if item.ins_class != "CONT":
            raise HTTPException(status_code=422, detail="TQ_NOT_CONT_SYMBOL")
        if item.underlying_symbol is None:
            raise HTTPException(status_code=422, detail="TQ_UNDERLYING_SYMBOL_EMPTY")
        return TqUnderlyingItem(
            symbol=item.symbol,
            underlying_symbol=item.underlying_symbol,
            ins_class=item.ins_class,
            exchange_id=item.exchange_id,
            product_id=item.product_id,
        )

    def _map_tq_exception(self, exc: Exception) -> HTTPException:
        message = str(exc)
        logger.bind(error_type=type(exc).__name__).warning(
            "TQ call failed: {}", message
        )
        if "adj_type" in message or "复权" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_ADJ_TYPE")
        if "K线数据周期" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_DURATION_SECONDS")
        if "序列长度" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_DATA_LENGTH")
        if "不能为空" in message or "参数错误" in message:
            return HTTPException(status_code=400, detail="TQ_INVALID_SYMBOL")
        return HTTPException(status_code=502, detail="TQ_NETWORK_UNAVAILABLE")


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return text


tq_manager = TqManager(config.tq)
