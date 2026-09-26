from typing import Any, Literal

from pydantic import BaseModel, ConfigDict


class CacheSeriesSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["ohlcv", "calendar", "main_mapping", "transition"]
    identity: dict[str, str]
    time_unit: Literal["ms", "ns", "date"]
    start: int | str | None
    end: int | str | None
    count: int
    total_count: int
    segment_count: int


class CacheSummaryResponse(BaseModel):
    items: list[CacheSeriesSummary]


class CachePruneResponse(BaseModel):
    status: Literal["completed", "partial"]
    rules: dict[str, Any]
    ohlcv: dict[str, int]
    auxiliary: dict[str, str | int | None]
    errors: list[dict[str, Any]]
