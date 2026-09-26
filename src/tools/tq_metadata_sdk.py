"""只从 SDK 取得映射的行情时间边界，不读取当前合约关联。"""

from threading import Event

from fastapi import HTTPException

from src.tools import tq_data_source
from src.tools.tq_ohlcv_validation import validate_records
from src.tools.tq_serial import get_serial
from src.types_tq import TqOhlcvRequest


def reference_time(api, symbol: str, deadline: float, stop: Event) -> int:
    frame = get_serial(
        api,
        TqOhlcvRequest(
            symbol=symbol, duration_seconds=300, data_length=1, enable_cache=False
        ),
        deadline,
        stop,
    )
    try:
        records = tq_data_source.clean_tq_serial_records(frame, "kline")
        validate_records(records, symbol, 300)
        if len(records) != 1:
            raise ValueError()
        timestamp = records[0]["datetime"]
        if type(timestamp) is not int or timestamp <= 0:
            raise ValueError()
    except (ValueError, TypeError, KeyError, HTTPException) as exc:
        raise HTTPException(502, "TQ_MAPPING_REFERENCE_UNAVAILABLE") from exc
    return timestamp
