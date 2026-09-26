"""TQ 单合约结果校验，不使用 interval 或数据库。"""

import math
import operator
from typing import Any

from fastapi import HTTPException


def validate_records(records: list[dict[str, Any]], symbol: str, duration: int) -> None:
    previous: tuple[int, int] | None = None
    for record in records:
        try:
            timestamp, sdk_id = record["datetime"], record["id"]
            if isinstance(timestamp, bool) or isinstance(sdk_id, bool):
                raise ValueError()
            timestamp, sdk_id = operator.index(timestamp), operator.index(sdk_id)
            if timestamp <= 0 or sdk_id < 0:
                raise ValueError()
            if previous is not None and (
                timestamp <= previous[0] or sdk_id != previous[1] + 1
            ):
                raise ValueError()
            if (
                record.get("symbol", symbol) != symbol
                or record.get("duration", duration) != duration
            ):
                raise ValueError()
            previous = timestamp, sdk_id
        except (KeyError, ValueError, TypeError, OverflowError) as exc:
            raise HTTPException(422, detail="TQ_INVALID_TIME_AXIS") from exc
        try:
            values = {
                key: None if record.get(key) is None else float(record[key])
                for key in (
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "open_oi",
                    "close_oi",
                )
            }
            if any(
                value is not None and not math.isfinite(value)
                for value in values.values()
            ):
                raise ValueError()
            if any(
                (value := values[key]) is not None and value < 0
                for key in ("volume", "open_oi", "close_oi")
            ):
                raise ValueError()
            for left, right in (
                ("high", "low"),
                ("high", "open"),
                ("high", "close"),
                ("open", "low"),
                ("close", "low"),
            ):
                left_value, right_value = values[left], values[right]
                if (
                    left_value is not None
                    and right_value is not None
                    and left_value < right_value
                ):
                    raise ValueError()
        except (ValueError, TypeError, OverflowError) as exc:
            raise HTTPException(422, detail="TQ_INVALID_OHLCV_VALUES") from exc
