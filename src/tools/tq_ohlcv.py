"""TQ 固定窗口与项目缓存的业务编排，不推算交易间隔。"""

import re
from typing import Any

from fastapi import HTTPException
from loguru import logger

from src.cache_tool import DuckDbOhlcvCache, TqOhlcvBatch, TqOhlcvSeries
from src.domain_errors import CacheCapacityExceeded
from src.tools.tq_ohlcv_validation import validate_records
from src.types_tq import TqOhlcvRequest


def cache_result(
    request: TqOhlcvRequest,
    records: list[dict[str, Any]],
    cache: DuckDbOhlcvCache | None,
) -> list[dict[str, Any]]:
    validate_records(records, request.symbol, request.duration_seconds)
    result = records[-request.data_length :]
    if (
        request.enable_cache
        and request.duration_seconds <= 604800
        and cache is not None
    ):
        series = TqOhlcvSeries(
            request.symbol, request.duration_seconds, request.adj_type
        )
        batch = TqOhlcvBatch(records)
        try:
            cache.write_tq_segment(series, batch)
        except CacheCapacityExceeded:
            raise
        except Exception:
            logger.bind(series_key=series.key).exception("TQ cache write failed")
        if records and len(records) < request.data_length:
            try:
                history = cache.read_connected_history(
                    series.key, batch, request.data_length
                ).rows
            except Exception:
                logger.bind(series_key=series.key).warning(
                    "TQ cache read failed; continuing as miss"
                )
                history = []
            merged = {row["datetime"]: row for row in history}
            merged.update({row["datetime"]: row for row in records})
            upper = records[-1]["datetime"]
            result = [merged[t] for t in sorted(merged) if t <= upper][
                -request.data_length :
            ]
    validate_records(result, request.symbol, request.duration_seconds)
    return result


def is_network_failure(exc: HTTPException) -> bool:
    return exc.detail in ("TQ_DATA_TIMEOUT", "TQ_NETWORK_UNAVAILABLE")


def status_symbol(symbol: str) -> tuple[str, bool]:
    if symbol.startswith("KQ.i@"):
        return symbol.replace("KQ.i@", "KQ.m@", 1), True
    if symbol.startswith("KQ.m@"):
        return symbol, True
    return require_actual_symbol(symbol), False


def require_actual_symbol(symbol: str) -> str:
    if not re.fullmatch(r"(?:SHFE|INE|DCE|CZCE|CFFEX|GFEX)\.[A-Za-z]+\d+", symbol):
        raise HTTPException(502, detail="TQ_TRADING_STATUS_UNAVAILABLE")
    return symbol


def closed_market_result(
    request: TqOhlcvRequest, cache: DuckDbOhlcvCache | None
) -> list[dict[str, Any]]:
    if cache is None:
        raise HTTPException(500, detail="TQ_CACHE_READ_FAILED")
    series = TqOhlcvSeries(request.symbol, request.duration_seconds, request.adj_type)
    try:
        records = cache.read_contiguous_before(
            series.key, 2**63 - 1, request.data_length
        ).rows
    except Exception as exc:
        raise HTTPException(500, detail="TQ_CACHE_READ_FAILED") from exc
    validate_records(records, request.symbol, request.duration_seconds)
    return records
