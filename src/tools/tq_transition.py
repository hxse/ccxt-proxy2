"""只从同一次旧合约 SDK 窗口定位并保存换月后的完整目标窗口。"""

from datetime import date

from fastapi import HTTPException

from src.cache_tool import TqOhlcvBatch
from src.cache_tool.models import canonical_row
from src.cache_tool.transition_store import TransitionIdentity
from src.responses_tq import TqTransitionRecord
from src.tools.tq_metadata_conversion import trading_candidate
from src.tq_validation import MAX_TQ_DATA_LENGTH, transition_duration
from src.types_tq import TqOhlcvRequest


def validate_prices(records):
    previous = -1
    try:
        for row in records:
            normalized = canonical_row(
                [
                    row["datetime"],
                    *(
                        row[f"old_{key}"]
                        for key in ("open", "high", "low", "close", "volume")
                    ),
                ]
            )
            if normalized[0] <= previous:
                raise ValueError()
            previous = normalized[0]
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise HTTPException(422, "TQ_INVALID_TRANSITION_WINDOW") from exc


async def attach_transitions(query, response, request, context, deadline):
    duration = transition_duration(request.transition_timeframe)
    for node in response.history:
        prices = await _window(query, node, request, context, duration, deadline)
        node.transition = [TqTransitionRecord.model_validate(row) for row in prices]
    return response


async def _window(query, node, request, context, duration, deadline):
    if node.old_symbol is None:
        return []
    identity = TransitionIdentity(
        request.symbol,
        date.fromisoformat(node.date),
        node.underlying_symbol,
        node.old_symbol,
        duration,
    )
    count = request.transition_bars
    if request.enable_cache:
        cached = await query._read("read_transition_prefix", identity, count)
        if cached is not None:
            validate_prices(cached)
            return cached
    raw_request = TqOhlcvRequest(
        symbol=node.old_symbol,
        duration_seconds=duration,
        data_length=MAX_TQ_DATA_LENGTH,
        enable_cache=False,
    )
    try:
        raw = await query.raw_fetch(raw_request, deadline)
    except HTTPException as exc:
        if isinstance(exc.detail, str) and exc.detail in {
            "TQ_INVALID_TIME_AXIS",
            "TQ_INVALID_OHLCV_VALUES",
        }:
            raise HTTPException(422, "TQ_INVALID_TRANSITION_WINDOW") from exc
        raise
    candidates = [trading_candidate(row["datetime"]) for row in raw]
    start = next(
        (i for i, day in enumerate(candidates) if day >= identity.roll_date), None
    )
    if start is None or start == 0 or candidates[start] != identity.roll_date:
        return []
    # 只使用该次副本已经提供的前驱，不寻找更老的窗口。
    calendar = await query.fetch_calendar_range(
        candidates[start - 1],
        identity.roll_date,
        context,
        enable_cache=request.enable_cache,
    )
    trading_days = [row.date for row in calendar.records if row.trading]
    previous_day = next(
        (day for day in trading_days if day >= candidates[start - 1]), None
    )
    if (
        previous_day is None
        or previous_day >= identity.roll_date
        or identity.roll_date not in trading_days
    ):
        return []
    target = raw[start : start + count + 1]
    prices = [
        {
            "datetime": row["datetime"],
            **{
                f"old_{key}": row.get(key)
                for key in ("open", "high", "low", "close", "volume")
            },
        }
        for row in target
    ]
    validate_prices(prices)
    if any(
        b["id"] != a["id"] + 1 for a, b in zip(raw[start - 1 : start + count], target)
    ):
        raise HTTPException(422, "TQ_INVALID_TRANSITION_WINDOW")
    if len(target) == count + 1 and request.enable_cache:
        await query._write(
            "submit_transition_window", identity, count, TqOhlcvBatch(target)
        )
    return prices[:count]
