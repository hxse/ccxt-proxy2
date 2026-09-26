"""明确日期的日历年回退；不读取任何时钟。"""

import calendar
from datetime import date


def years_before(day: date, years: int) -> date:
    if years >= day.year:
        return date.min
    year = day.year - years
    return date(year, day.month, min(day.day, calendar.monthrange(year, day.month)[1]))
