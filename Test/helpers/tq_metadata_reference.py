"""唯一旧元数据参考入口：固定离线源并恢复 SDK 全局状态。"""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

from tqsdk import calendar


def reference(holidays, mappings, start, end, symbol):
    def get(url, **kwargs):
        raw = mappings if "continuous" in url else holidays
        return SimpleNamespace(json=lambda: raw, raise_for_status=lambda: None)

    with ExitStack() as stack:
        stack.enter_context(patch.object(calendar, "rest_days_df", None))
        stack.enter_context(patch.object(calendar, "chinese_holidays_range", None))
        stack.enter_context(patch.object(calendar.TqContCalendar, "continuous", None))
        stack.enter_context(patch.object(calendar.requests, "get", get))
        calendar_frame = calendar._get_trading_calendar(start, end)
        mapping_frame = calendar.TqContCalendar(start, end, [symbol]).df
        return (
            [
                (row.date.date(), bool(row.trading))
                for row in calendar_frame.itertuples()
            ],
            [
                (row["date"].date(), row[symbol])
                for row in mapping_frame.to_dict("records")
            ],
        )
