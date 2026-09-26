import json
import math
import operator
from dataclasses import dataclass
from typing import Any, Iterable, Sequence, cast

MAX_RESPONSE_ROWS = 100_000
OhlcvRow = tuple[int, float, float, float, float, float]


@dataclass(frozen=True, slots=True)
class OhlcvSeries:
    provider: str
    mode: str
    market: str
    symbol: str
    timeframe: str
    variant: str = "default"

    @property
    def key(self) -> str:
        return json.dumps(
            {
                "market": self.market,
                "mode": self.mode,
                "provider": self.provider,
                "symbol": self.symbol,
                "timeframe": self.timeframe,
                "variant": self.variant,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


@dataclass(frozen=True, slots=True)
class OhlcvResult:
    rows: list[OhlcvRow]
    last_bar_completion_confirmed: bool | None

    def __post_init__(self) -> None:
        if not self.rows and self.last_bar_completion_confirmed is not None:
            raise ValueError("empty OHLCV result must use null completion metadata")
        if self.rows and self.last_bar_completion_confirmed is None:
            raise ValueError("non-empty OHLCV result requires completion metadata")


def canonical_row(values: Sequence[object]) -> OhlcvRow:
    if len(values) < 6:
        raise ValueError("OHLCV row requires six values")
    if isinstance(values[0], bool):
        raise ValueError("OHLCV time must be an integer")
    try:
        timestamp = operator.index(cast(Any, values[0]))
    except TypeError as exc:
        raise ValueError("OHLCV time must be an integer") from exc
    numbers = tuple(float(cast(Any, value)) for value in values[1:6])
    if timestamp < 0 or not all(math.isfinite(value) for value in numbers):
        raise ValueError("OHLCV values must be finite")
    open_, high, low, close, volume = numbers
    if volume < 0 or high < low or high < max(open_, close) or low > min(open_, close):
        raise ValueError("OHLCV price/volume relationship is invalid")
    return timestamp, open_, high, low, close, volume


def canonical_rows(rows: Iterable[Sequence[object]]) -> list[OhlcvRow]:
    by_time: dict[int, OhlcvRow] = {}
    for values in rows:
        row = canonical_row(values)
        by_time[row[0]] = row
    return [by_time[timestamp] for timestamp in sorted(by_time)]


def merge_rows(*groups: Iterable[OhlcvRow]) -> list[OhlcvRow]:
    by_time: dict[int, OhlcvRow] = {}
    for group in groups:
        for row in group:
            by_time[row[0]] = row
    return [by_time[timestamp] for timestamp in sorted(by_time)]


@dataclass(frozen=True, slots=True)
class TqOhlcvSeries:
    symbol: str
    duration_seconds: int
    adj_type: str | None = None

    def __post_init__(self) -> None:
        if (
            not self.symbol
            or isinstance(self.duration_seconds, bool)
            or not isinstance(self.duration_seconds, int)
            or self.duration_seconds <= 0
        ):
            raise ValueError("invalid TQ series")
        if self.adj_type not in (None, "", "F", "FORWARD", "B", "BACK"):
            raise ValueError("invalid TQ adjustment")

    @property
    def key(self) -> str:
        variant = {None: "default", "": "default", "FORWARD": "F", "BACK": "B"}.get(
            self.adj_type, self.adj_type
        )
        return OhlcvSeries(
            "tq",
            "live",
            "future",
            self.symbol,
            f"{self.duration_seconds}s",
            str(variant),
        ).key


@dataclass(frozen=True, slots=True)
class TqOhlcvBatch:
    records: list[dict[str, Any]]
    last_bar_completion_confirmed: bool | None = False


@dataclass(frozen=True, slots=True)
class CachedHistory[T]:
    rows: list[T]


@dataclass(frozen=True, slots=True)
class SeriesSummary:
    start: int | None
    end: int | None
    count: int
    total_count: int
    segment_count: int
    time_unit: str | None


def eligible_rows[T](rows: list[T], confirmed: bool | None) -> list[T]:
    """统一尾根筛选；已确认数据库行不需要再次提交。"""
    return rows if confirmed is True else rows[:-1]


def tq_storage_rows(series: TqOhlcvSeries, batch: TqOhlcvBatch) -> list[tuple]:
    rows = []
    try:
        for record in eligible_rows(batch.records, batch.last_bar_completion_confirmed):
            if record.get("symbol", series.symbol) != series.symbol:
                raise ValueError("TQ symbol differs from series")
            if (
                record.get("duration", series.duration_seconds)
                != series.duration_seconds
            ):
                raise ValueError("TQ duration differs from series")
            sdk_id = record["id"]
            if isinstance(sdk_id, bool) or operator.index(sdk_id) < 0:
                raise ValueError("invalid SDK id")
            row = canonical_row(
                [
                    record[key]
                    for key in ("datetime", "open", "high", "low", "close", "volume")
                ]
            )
            oi = tuple(
                None if record.get(key) is None else float(record[key])
                for key in ("open_oi", "close_oi")
            )
            if any(
                value is not None and (not math.isfinite(value) or value < 0)
                for value in oi
            ):
                raise ValueError("invalid open interest")
            if rows and (row[0] <= rows[-1][0] or sdk_id != rows[-1][6] + 1):
                raise ValueError("invalid TQ sequence")
            rows.append((*row, int(sdk_id), *oi))
    except (KeyError, TypeError, ValueError, OverflowError):
        return []
    return rows
