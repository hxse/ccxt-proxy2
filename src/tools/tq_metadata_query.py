"""日历按覆盖刷新，映射每次读取官方事件源；SDK 只提供行情日期。"""

import asyncio
from dataclasses import asdict, replace
from time import monotonic

from fastapi import HTTPException
from loguru import logger

from src.cache_tool.metadata_models import CalendarFacts, validate_calendar
from src.responses_tq import (
    TqUnderlyingHistoryItem,
    TqUnderlyingItem,
    TqUnderlyingSymbolResponse,
)
from src.tools.public_time import fetch_public_time
from src.tools.tq_metadata_conversion import (
    MetadataContext,
    calendar_range,
    mapping_range,
    natural_date,
    trading_candidate,
)
from src.tools.tq_metadata_source import MetadataSource
from src.tools.tq_transition import attach_transitions

TRANSITION_TIMEOUT_SECONDS = 45.0


class TqMetadataQuery:
    def __init__(self, cache, headers, reference, raw_fetch=None, *, proxy_url=None):
        self.cache = cache
        self.source = MetadataSource(proxy_url=proxy_url)
        self._headers = headers
        self._reference = reference
        self.raw_fetch = raw_fetch

    async def _read(self, method: str, *args):
        if self.cache is None:
            return None
        try:
            return await asyncio.to_thread(getattr(self.cache, method), *args)
        except Exception:
            logger.bind(operation=method).warning("TQ metadata cache read failed")
            return None

    async def _write(self, method: str, *args):
        if self.cache is None:
            return
        try:
            await asyncio.to_thread(getattr(self.cache, method), *args)
        except Exception:
            logger.bind(operation=method).exception("TQ metadata cache write failed")

    async def _holiday_source(self, context: MetadataContext):
        if context.calendar is None:
            context.calendar = await self.source.fetch(
                "calendar", await self._headers()
            )
        return context.calendar

    async def fetch_calendar_range(
        self, start_date, end_date, context: MetadataContext, *, enable_cache=True
    ):
        if context.calendar is not None:
            return calendar_range(
                start_date, end_date, context.calendar, context.server_time
            )
        if enable_cache:
            facts = await self._read("read_metadata_facts", "calendar")
            if (
                isinstance(facts, CalendarFacts)
                and natural_date(context.server_time) < facts.holiday_last
                and facts.valid_from <= start_date <= end_date <= facts.valid_to
            ):
                cached = await self._read("read_calendar_range", start_date, end_date)
                if cached is not None:
                    validate_calendar(cached.records, start_date, end_date)
                    return cached
        source = await self._holiday_source(context)
        result = calendar_range(start_date, end_date, source, context.server_time)
        validate_calendar(result.records, start_date, end_date)
        return result

    async def calendar(self, request):
        context = MetadataContext((await fetch_public_time()).serverTime)
        result = await self.fetch_calendar_range(
            request.start_date,
            request.end_date,
            context,
            enable_cache=request.enable_cache,
        )
        if request.enable_cache and context.calendar is not None:
            await self._write("submit_calendar", result)
        validate_calendar(result.records, request.start_date, request.end_date)
        return [
            {"date": row.date.isoformat(), "trading": row.trading}
            for row in result.records
        ]

    async def fetch_main_mapping_range(
        self,
        symbol,
        start_date,
        end_date,
        context: MetadataContext,
        *,
        enable_cache=True,
    ):
        assert context.reference_time is not None and context.mapping is not None
        reference_day = trading_candidate(context.reference_time)
        lower, upper = min(start_date, reference_day), max(end_date, reference_day)
        calendar = await self.fetch_calendar_range(
            lower, upper, context, enable_cache=enable_cache
        )
        calendar_fetched = context.calendar is not None
        source = await self._holiday_source(context)
        # 转换先于缓存复用：旧片段可能尚未覆盖官方已经修订的历史节点。
        result = mapping_range(symbol, start_date, end_date, context)
        if enable_cache:
            fresh_calendar = calendar_range(lower, upper, source, context.server_time)
            if (
                calendar_fetched
                or calendar.facts.digest != fresh_calendar.facts.digest
                or calendar.records != fresh_calendar.records
            ):
                await self._write("submit_calendar", fresh_calendar)
            cached = await self._read("read_matching_mapping", result)
            if cached is not None:
                return replace(result, records=cached.records, nodes=cached.nodes)
            await self._write("submit_mapping", result)
        return result

    async def mapping(self, request):
        if not request.symbol.startswith("KQ.m@"):
            raise HTTPException(422, "TQ_NOT_CONT_SYMBOL")
        if request.transition_timeframe is None:
            return await self._mapping(request, None)
        try:
            async with asyncio.timeout(TRANSITION_TIMEOUT_SECONDS):
                return await self._mapping(
                    request, monotonic() + TRANSITION_TIMEOUT_SECONDS
                )
        except TimeoutError:
            raise HTTPException(504, "TQ_TRANSITION_TIMEOUT") from None

    async def _mapping(self, request, deadline):
        context = MetadataContext((await fetch_public_time()).serverTime)
        context.mapping = await self.source.fetch("mapping", await self._headers())
        if request.symbol not in context.mapping.events:
            raise HTTPException(422, "TQ_NOT_CONT_SYMBOL")
        context.reference_time = await self._reference(request.symbol)
        reference_day = trading_candidate(context.reference_time)
        start = (
            trading_candidate(request.start_time)
            if request.start_time is not None
            else reference_day
        )
        end = (
            trading_candidate(request.end_time)
            if request.end_time is not None
            else reference_day
        )
        mapping_result = await self.fetch_main_mapping_range(
            request.symbol, start, end, context, enable_cache=request.enable_cache
        )
        item = TqUnderlyingItem(
            symbol=request.symbol,
            underlying_symbol=mapping_result.facts.underlying_symbol,
            ins_class="CONT",
            exchange_id="KQ",
            product_id="",
        )
        if request.start_time is None:
            return TqUnderlyingSymbolResponse(items=[item])
        history = [
            TqUnderlyingHistoryItem(
                symbol=request.symbol,
                **(asdict(node) | {"date": node.date.isoformat()}),
            )
            for node in mapping_result.nodes
        ]
        result = TqUnderlyingSymbolResponse(items=[item], history=history)
        if request.transition_timeframe is not None:
            return await attach_transitions(self, result, request, context, deadline)
        return result

    async def close(self):
        await self.source.close()
