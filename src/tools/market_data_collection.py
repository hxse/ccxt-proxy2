"""固定窗口采集；数据校验、连续性及落盘完全归业务路由。"""

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from loguru import logger

from src.tools.market_data_dates import years_before
from src.tools.market_data_http import JobRequestError, MarketDataHttp
from src.tools.market_data_types import TqCollectionPlan

DURATIONS = {"5m": 300, "1h": 3600, "1d": 86400, "1w": 604800}


@dataclass
class JobResult:
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0

    def add(self, other):
        self.succeeded += other.succeeded
        self.failed += other.failed
        self.skipped += other.skipped

    def failure(self, exc: JobRequestError, **identity):
        self.failed += 1
        logger.bind(**identity, error_code=exc.code, status_code=exc.status).warning(
            "market data request failed"
        )


def ohlcv_range(data):
    # 这里只确认派生参数可安全使用；不能自行推算时间轴或另建缓存证明。
    if not isinstance(data, list):
        raise JobRequestError("BACKGROUND_INVALID_OHLCV_RESPONSE")
    times: list[int] = []
    for row in data:
        timestamp = row.get("datetime") if isinstance(row, dict) else None
        if type(timestamp) is not int or timestamp <= 0:
            raise JobRequestError("BACKGROUND_INVALID_OHLCV_RESPONSE")
        times.append(timestamp)
    if any(a >= b for a, b in zip(times, times[1:])):
        raise JobRequestError("BACKGROUND_INVALID_OHLCV_RESPONSE")
    return (times[0], times[-1]) if times else None


async def _ohlcv(client, plan, symbol, period, result):
    try:
        data = await client.request(
            "GET",
            "/tq/fetch_ohlcv",
            params={
                "symbol": symbol,
                "duration_seconds": DURATIONS[period],
                "data_length": plan.data_length,
                "enable_cache": True,
            },
        )
        bounds = ohlcv_range(data)
        result.succeeded += 1
        logger.bind(symbol=symbol, period=period, count=len(data)).info(
            "market data collected"
        )
        return bounds
    except JobRequestError as exc:
        result.failure(exc, symbol=symbol, period=period)
        return None


async def collect_market_data(
    client: MarketDataHttp, plan: TqCollectionPlan, *, enabled=True
):
    result = JobResult()
    if not enabled or not plan.has_work:
        result.skipped = 1
        return result
    for symbol in plan.symbols.values():
        for period in plan.timeframes:
            bounds = (
                await _ohlcv(client, plan, symbol, period, result)
                if plan.save_main
                else None
            )
            if (
                bounds is not None
                and plan.save_mapping
                and period in plan.mapping_timeframes
            ):
                params = {
                    "symbol": symbol,
                    "start_time": bounds[0],
                    "end_time": bounds[1],
                    "enable_cache": True,
                }
                if plan.save_transition and period in plan.transition_timeframes:
                    params["transition_timeframe"] = period
                try:
                    data = await client.request(
                        "GET", "/tq/fetch_underlying_symbol", params=params
                    )
                    if not isinstance(data, dict) or not isinstance(
                        data.get("history"), list
                    ):
                        raise JobRequestError("BACKGROUND_INVALID_MAPPING_RESPONSE")
                    result.succeeded += 1
                    logger.bind(
                        symbol=symbol, period=period, count=len(data["history"])
                    ).info("main mapping collected")
                except JobRequestError as exc:
                    result.failure(exc, symbol=symbol, period=period)
            if plan.save_weighted:
                await _ohlcv(
                    client, plan, symbol.replace("KQ.m@", "KQ.i@", 1), period, result
                )
    if plan.save_calendar:
        try:
            clock = await client.request("GET", "/system/fetch_time")
            if (
                not isinstance(clock, dict)
                or type(clock.get("serverTime")) is not int
                or clock["serverTime"] <= 0
            ):
                raise JobRequestError("BACKGROUND_INVALID_TIME_RESPONSE")
            try:
                today = datetime.fromtimestamp(
                    clock["serverTime"] / 1000, ZoneInfo("Asia/Shanghai")
                ).date()
            except (OverflowError, OSError, ValueError):
                raise JobRequestError("BACKGROUND_INVALID_TIME_RESPONSE") from None
            data = await client.request(
                "GET",
                "/tq/fetch_trading_calendar",
                params={
                    "start_date": years_before(
                        today, plan.calendar_lookback_years
                    ).isoformat(),
                    "end_date": today.isoformat(),
                    "enable_cache": True,
                },
            )
            if not isinstance(data, list):
                raise JobRequestError("BACKGROUND_INVALID_CALENDAR_RESPONSE")
            result.succeeded += 1
            logger.bind(count=len(data)).info("trading calendar collected")
        except JobRequestError as exc:
            result.failure(exc, operation="calendar")
    return result
