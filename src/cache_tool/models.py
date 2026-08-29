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
