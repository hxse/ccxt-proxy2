"""TQ 请求的共用参数校验；不获取行情。"""

import re
from datetime import date
from typing import Literal

from fastapi import HTTPException

TqAdjType = Literal["F", "B", "FORWARD", "BACK"]

DEFAULT_TQ_DATA_LENGTH = 10000
MAX_TQ_DATA_LENGTH = 10000
MAX_TQ_OHLCV_LENGTH = 100000
TQ_ADJ_TYPE_QUERY_ENUM = ["", "F", "B", "FORWARD", "BACK"]


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized = [symbol.strip() for symbol in symbols]
    if not normalized or any(not symbol for symbol in normalized):
        raise ValueError("TQ_INVALID_SYMBOL")
    return normalized


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not normalized:
        raise ValueError("TQ_INVALID_SYMBOL")
    return normalized


def _normalize_duration_seconds(duration_seconds: int) -> int:
    if duration_seconds <= 0:
        raise ValueError("TQ_INVALID_DURATION_SECONDS")
    if duration_seconds > 86400 and duration_seconds % 86400 != 0:
        raise ValueError("TQ_INVALID_DURATION_SECONDS")
    return duration_seconds


def _http_validation_error(code: str) -> HTTPException:
    return HTTPException(status_code=400, detail=code)


def _validate_symbols(symbols: list[str]) -> list[str]:
    try:
        return _normalize_symbols(symbols)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_symbol(symbol: str) -> str:
    try:
        return _normalize_symbol(symbol)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_duration_seconds(duration_seconds: int) -> int:
    try:
        return _normalize_duration_seconds(duration_seconds)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_data_length(data_length: int, maximum: int = MAX_TQ_DATA_LENGTH) -> int:
    if data_length < 1 or data_length > maximum:
        raise _http_validation_error("TQ_INVALID_DATA_LENGTH")
    return data_length


def _validate_adj_type(adj_type: str | None) -> TqAdjType | None:
    if adj_type == "":
        return None
    if adj_type is None:
        return None
    if adj_type == "F":
        return "F"
    if adj_type == "B":
        return "B"
    if adj_type == "FORWARD":
        return "FORWARD"
    if adj_type == "BACK":
        return "BACK"
    raise HTTPException(status_code=400, detail="TQ_INVALID_ADJ_TYPE")


def _validate_calendar_range(start_date: date, end_date: date) -> None:
    if start_date > end_date:
        raise _http_validation_error("TQ_INVALID_DATE_RANGE")


def transition_duration(timeframe: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([smhdw])", timeframe)
    if not match:
        raise ValueError("TQ_INVALID_TRANSITION_TIMEFRAME")
    duration = (
        int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match[2]]
    )
    try:
        return _normalize_duration_seconds(duration)
    except ValueError as exc:
        raise ValueError("TQ_INVALID_TRANSITION_TIMEFRAME") from exc
