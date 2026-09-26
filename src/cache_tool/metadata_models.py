"""日期数据与核验事实；不携带 SDK、网络函数或任意 DataFrame。"""

import json
from dataclasses import dataclass
from datetime import date, timedelta


def metadata_key(kind: str, symbol: str | None = None) -> str:
    identity = {"provider": "tq", "mode": "live"}
    if kind == "main_mapping" and symbol:
        identity["symbol"] = symbol
    elif kind != "calendar":
        raise ValueError("invalid metadata identity")
    return json.dumps(identity, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class CalendarDay:
    date: date
    trading: bool


@dataclass(frozen=True, slots=True)
class CalendarFacts:
    holiday_last: date
    valid_from: date
    valid_to: date
    digest: str
    server_time: int


@dataclass(frozen=True, slots=True)
class CalendarSourceResult:
    records: list[CalendarDay]
    facts: CalendarFacts


@dataclass(frozen=True, slots=True)
class MappingDay:
    date: date
    underlying_symbol: str


@dataclass(frozen=True, slots=True)
class MappingNode:
    date: date
    underlying_symbol: str
    old_symbol: str | None


@dataclass(frozen=True, slots=True)
class MappingFacts:
    digest: str
    calendar_digest: str
    server_time: int
    verified_date: date
    underlying_symbol: str
    reference_time: int


@dataclass(frozen=True, slots=True)
class MappingSourceResult:
    symbol: str
    records: list[MappingDay]
    nodes: list[MappingNode]
    verification_node: MappingNode
    facts: MappingFacts


@dataclass(frozen=True, slots=True)
class CachedMapping:
    records: list[MappingDay]
    nodes: list[MappingNode]


def calendar_dates(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def validate_calendar(records: list[CalendarDay], start: date, end: date) -> None:
    if start > end or [row.date for row in records] != calendar_dates(start, end):
        raise ValueError("calendar range is incomplete")
    if any(type(row.trading) is not bool for row in records):
        raise ValueError("calendar trading must be boolean")


def mapping_nodes_for(
    records: list[MappingDay], nodes: list[MappingNode]
) -> list[MappingNode]:
    selected = {}
    for row in records:
        candidates = [node for node in nodes if node.date <= row.date]
        if not candidates or candidates[-1].underlying_symbol != row.underlying_symbol:
            raise ValueError("mapping node context is incomplete")
        node = candidates[-1]
        selected[node.date] = node
    return list(selected.values())
