"""本地元数据概况，调用方只能提供声明的身份过滤项。"""

import json
from datetime import date

FILTERS = {"provider", "mode", "market", "symbol", "timeframe", "variant", "kind"}


def list_summaries(connection, filters: dict[str, str] | None = None):
    filters = filters or {}
    if not set(filters) <= FILTERS or any(
        not isinstance(value, str) or not value for value in filters.values()
    ):
        raise ValueError("invalid cache summary filter")
    rows = connection.execute("""
        WITH ranked AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY series_key,data_kind
                ORDER BY last_time DESC,updated_at DESC,segment_id ASC) AS position,
                SUM(row_count) OVER (PARTITION BY series_key,data_kind) AS total_count,
                COUNT(*) OVER (PARTITION BY series_key,data_kind) AS segment_count
            FROM cache_segments
        ) SELECT series_key,data_kind,time_unit,first_time,last_time,row_count,
                 total_count,segment_count FROM ranked WHERE position=1
        ORDER BY data_kind,series_key
    """).fetchall()
    result = []
    for key, kind, unit, first, last, count, total, segments in rows:
        identity = json.loads(key)
        if any(
            (kind if name == "kind" else identity.get(name)) != value
            for name, value in filters.items()
        ):
            continue
        result.append(
            {
                "kind": kind,
                "identity": identity,
                "time_unit": unit,
                "start": date.fromordinal(first).isoformat()
                if unit == "date"
                else first,
                "end": date.fromordinal(last).isoformat() if unit == "date" else last,
                "count": count,
                "total_count": total,
                "segment_count": segments,
            }
        )
    return result
