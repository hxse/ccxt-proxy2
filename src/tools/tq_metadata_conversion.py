"""官方元数据的显式日期转换，不读取本机日期。

基于 TqSdk 3.10.2 calendar.py（mayanqiong）及 datetime.py，Apache-2.0。
修改：纯函数、整数纳秒、严格校验、真实节点与已生效边界；许可见 LICENSES/tqsdk.txt。
"""

import bisect
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from src.cache_tool.metadata_models import (
    CalendarDay,
    CalendarFacts,
    CalendarSourceResult,
    MappingDay,
    MappingFacts,
    MappingNode,
    MappingSourceResult,
    calendar_dates,
    mapping_nodes_for,
)

CST = ZoneInfo("Asia/Shanghai")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True)
class HolidaySource:
    holidays: frozenset[date]
    digest: str

    @property
    def last(self) -> date:
        return max(self.holidays)

    @property
    def start(self) -> date:
        return date(min(self.holidays).year, 1, 1)

    @property
    def end(self) -> date:
        return date(self.last.year, 12, 31)


@dataclass(frozen=True)
class MappingSource:
    events: dict[str, tuple[tuple[date, str], ...]]
    digest: str


@dataclass
class MetadataContext:
    server_time: int
    calendar: HolidaySource | None = None
    mapping: MappingSource | None = None
    reference_time: int | None = None


def _digest(data) -> str:
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def parse_holidays(raw) -> HolidaySource:
    try:
        if not isinstance(raw, list) or not raw:
            raise ValueError()
        days = set()
        for item in raw:
            if not isinstance(item, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", item
            ):
                raise ValueError()
            days.add(date.fromisoformat(item))
        return HolidaySource(frozenset(days), _digest(raw))
    except (TypeError, ValueError) as exc:
        raise HTTPException(502, "TQ_METADATA_INVALID_SOURCE") from exc


def parse_mapping(raw) -> MappingSource:
    try:
        if not isinstance(raw, dict) or not raw:
            raise ValueError()
        result = {}
        for symbol, events in raw.items():
            if not isinstance(symbol, str) or not re.fullmatch(
                r"[A-Z]+\.[A-Za-z]+", symbol
            ):
                raise ValueError()
            if not isinstance(events, list) or not events:
                raise ValueError()
            by_date = {}
            for event in events:
                if not isinstance(event, (list, tuple)) or len(event) != 2:
                    raise ValueError()
                stamp, underlying = event
                if (
                    isinstance(stamp, bool)
                    or not isinstance(stamp, (str, int))
                    or not re.fullmatch(r"\d{8}", str(stamp))
                ):
                    raise ValueError()
                day = datetime.strptime(str(stamp), "%Y%m%d").date()
                if not isinstance(underlying, str) or (
                    underlying and not re.fullmatch(r"[A-Z]+\.[A-Za-z]+\d+", underlying)
                ):
                    raise ValueError()
                if day in by_date and by_date[day] != underlying:
                    raise ValueError()
                by_date[day] = underlying
            ordered = tuple(sorted(by_date.items()))
            seen = False
            for _, underlying in ordered:
                if seen and not underlying:
                    raise ValueError()
                seen = seen or bool(underlying)
            result[f"KQ.m@{symbol}"] = ordered
        return MappingSource(result, _digest(raw))
    except (ValueError, TypeError) as exc:
        raise HTTPException(502, "TQ_METADATA_INVALID_SOURCE") from exc


def ns_datetime(timestamp: int) -> datetime:
    if isinstance(timestamp, bool) or not isinstance(timestamp, int) or timestamp <= 0:
        raise ValueError("positive integer nanoseconds required")
    # 不经 float；日期转换不需要舍入到小数秒。
    return (EPOCH + timedelta(seconds=timestamp // 1_000_000_000)).astimezone(CST)


def natural_date(server_time: int) -> date:
    return (EPOCH + timedelta(milliseconds=server_time)).astimezone(CST).date()


def trading_candidate(timestamp: int) -> date:
    value = ns_datetime(timestamp)
    day = value.date() + timedelta(days=value.hour >= 18)
    if day.weekday() >= 5:
        day += timedelta(days=7 - day.weekday())
    return day


def next_trading(day: date, calendar: HolidaySource) -> date:
    while calendar.start <= day <= calendar.end:
        if day.weekday() < 5 and day not in calendar.holidays:
            return day
        day += timedelta(days=1)
    raise HTTPException(422, "TQ_CALENDAR_RANGE_UNAVAILABLE")


def calendar_range(
    start: date, end: date, source: HolidaySource, server_time: int
) -> CalendarSourceResult:
    if not source.start <= start <= end <= source.end:
        raise HTTPException(422, "TQ_CALENDAR_RANGE_UNAVAILABLE")
    records = [
        CalendarDay(day, day.weekday() < 5 and day not in source.holidays)
        for day in calendar_dates(start, end)
    ]
    return CalendarSourceResult(
        records,
        CalendarFacts(
            source.last, source.start, source.end, source.digest, server_time
        ),
    )


def verified_date(context: MetadataContext) -> date:
    assert context.calendar is not None
    try:
        if context.reference_time is None:
            raise ValueError()
        day = next_trading(trading_candidate(context.reference_time), context.calendar)
        candidate = trading_candidate(context.server_time * 1_000_000)
        upper = (
            candidate
            if candidate > context.calendar.end
            else next_trading(candidate, context.calendar)
        )
        if day > upper:
            raise ValueError()
        return day
    except (ValueError, OverflowError) as exc:
        raise HTTPException(502, "TQ_MAPPING_REFERENCE_UNAVAILABLE") from exc


def mapping_range(
    symbol: str, start: date, end: date, context: MetadataContext
) -> MappingSourceResult:
    calendar, source = context.calendar, context.mapping
    assert calendar is not None and source is not None
    day = verified_date(context)
    dates = [
        row.date
        for row in calendar_range(start, end, calendar, context.server_time).records
        if row.trading
    ]
    if dates and dates[-1] > day:
        raise HTTPException(422, "TQ_MAPPING_RANGE_UNAVAILABLE")
    if symbol not in source.events:
        raise HTTPException(422, "TQ_NOT_CONT_SYMBOL")
    # 非交易日连续发布的事件以实际首个交易日最终生效值为准。
    effective = {}
    unknown_left = None
    for stamp, underlying in source.events[symbol]:
        if stamp > day:
            continue
        if stamp < calendar.start:
            unknown_left = underlying or None
            continue
        stamp = next_trading(stamp, calendar)
        if stamp <= day:
            effective[stamp] = underlying
    nodes = []
    previous = unknown_left
    for stamp, underlying in sorted(effective.items()):
        if underlying and underlying != previous:
            nodes.append(MappingNode(stamp, underlying, previous))
        previous = underlying or previous
    node_dates = [node.date for node in nodes]

    def on_date(target):
        index = bisect.bisect_right(node_dates, target) - 1
        if index < 0:
            if unknown_left:
                raise HTTPException(502, "TQ_MAPPING_CONTEXT_UNAVAILABLE")
            return None
        return nodes[index]

    verification = on_date(day)
    if verification is None:
        raise HTTPException(502, "TQ_MAPPING_INCOMPLETE")
    records = []
    for target in dates:
        node = on_date(target)
        if node is not None:
            records.append(MappingDay(target, node.underlying_symbol))
    selected = mapping_nodes_for(records, nodes)
    assert context.reference_time is not None
    facts = MappingFacts(
        source.digest,
        calendar.digest,
        context.server_time,
        day,
        verification.underlying_symbol,
        context.reference_time,
    )
    return MappingSourceResult(symbol, records, selected, verification, facts)
