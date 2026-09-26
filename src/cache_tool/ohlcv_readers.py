"""普通行情的单快照读取；仅由缓存外观在 reader scope 内调用。"""

import json
from typing import Any

from src.cache_tool.models import (
    MAX_RESPONSE_ROWS,
    CachedHistory,
    OhlcvResult,
    SeriesSummary,
    TqOhlcvBatch,
    TqOhlcvSeries,
    canonical_row,
    eligible_rows,
    tq_storage_rows,
)


def _limit(max_rows: int) -> None:
    if (
        isinstance(max_rows, bool)
        or not isinstance(max_rows, int)
        or not 1 <= max_rows <= MAX_RESPONSE_ROWS
    ):
        raise ValueError("max_rows must be an integer in 1..100000")


def _decode(series_key: str, raw) -> CachedHistory:
    records: list[Any] = []
    identity = None
    for row in raw:
        if row[9] == "ns":
            if identity is None:
                identity = json.loads(series_key)
            record = dict(
                zip(
                    (
                        "datetime",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "id",
                        "open_oi",
                        "close_oi",
                    ),
                    row[:9],
                    strict=True,
                )
            )
            record.update(
                symbol=identity["symbol"], duration=int(identity["timeframe"][:-1])
            )
            records.append(record)
        else:
            records.append(canonical_row(row))
    return CachedHistory(records)


def _read(
    connection,
    series_key: str,
    end_time: int,
    max_rows: int,
    eligible: list[int] | None,
):
    _limit(max_rows)
    overlap = (
        ""
        if eligible is None
        else "AND EXISTS (SELECT 1 FROM ohlcv_rows x WHERE x.segment_id=s.segment_id AND x.time IN (SELECT unnest(?)))"
    )
    order = "MAX(r.time) DESC" if eligible is None else "COUNT(*) DESC"
    args = [series_key, end_time]
    if eligible is not None:
        args.append(eligible)
    args += [end_time, max_rows]
    raw = connection.execute(
        f"""
        WITH best AS (
            SELECT s.segment_id FROM cache_segments s
            JOIN ohlcv_rows r USING(segment_id)
            WHERE s.data_kind='ohlcv' AND s.series_key=? AND r.time<=? {overlap}
            GROUP BY s.segment_id, s.updated_at
            ORDER BY {order}, s.updated_at DESC, s.segment_id ASC LIMIT 1
        ), tail AS (
            SELECT r.time, r.open, r.high, r.low, r.close, r.volume,
                   r.sdk_id, r.open_oi, r.close_oi, s.time_unit
            FROM ohlcv_rows r JOIN best USING(segment_id)
            JOIN cache_segments s USING(segment_id)
            WHERE r.time<=? ORDER BY r.time DESC LIMIT ?
        ) SELECT * FROM tail ORDER BY time
    """,
        args,
    ).fetchall()
    return _decode(series_key, raw)


def read_before(connection, series_key: str, end_time: int, max_rows: int):
    return _read(connection, series_key, end_time, max_rows, None)


def read_connected(
    connection, series_key: str, batch: OhlcvResult | TqOhlcvBatch, max_rows: int
):
    _limit(max_rows)
    if isinstance(batch, TqOhlcvBatch):
        identity = json.loads(series_key)
        variant = identity["variant"]
        series = TqOhlcvSeries(
            identity["symbol"],
            int(identity["timeframe"][:-1]),
            None if variant == "default" else variant,
        )
        rows = tq_storage_rows(series, batch)
        upper = batch.records[-1]["datetime"] if batch.records else 0
    else:
        try:
            rows = [
                canonical_row(row)
                for row in eligible_rows(
                    batch.rows, batch.last_bar_completion_confirmed
                )
            ]
        except (TypeError, ValueError, OverflowError):
            rows = []
        upper = batch.rows[-1][0] if batch.rows else 0
    if not rows:
        return CachedHistory([])
    return _read(connection, series_key, upper, max_rows, [row[0] for row in rows])


def read_summary(connection, series_key: str) -> SeriesSummary:
    row = connection.execute(
        """
        WITH segments AS (
            SELECT * FROM cache_segments WHERE data_kind='ohlcv' AND series_key=?
        ), latest AS (
            SELECT first_time, last_time, row_count, time_unit FROM segments
            ORDER BY last_time DESC, updated_at DESC, segment_id ASC LIMIT 1
        ) SELECT latest.*, totals.total_count, totals.segment_count FROM
          (SELECT COALESCE(SUM(row_count),0) total_count, COUNT(*) segment_count FROM segments) totals
          LEFT JOIN latest ON true
    """,
        [series_key],
    ).fetchone()
    return SeriesSummary(row[0], row[1], row[2] or 0, row[4], row[5], row[3])


def read_best_prefix(connection, series_key: str, since: int, max_rows: int | None):
    limit_sql = "" if max_rows is None else " LIMIT ?"
    parameters: list[Any] = [series_key, since, since, since, since]
    if max_rows is not None:
        parameters.append(max_rows)
    query = f"""
        WITH best AS (
            SELECT s.segment_id
            FROM cache_segments AS s
            WHERE s.data_kind='ohlcv' AND s.time_unit='ms' AND s.series_key = ?
              AND s.covered_from <= ?
              AND s.last_time >= ?
            ORDER BY (
                SELECT COUNT(*) FROM ohlcv_rows AS c
                WHERE c.segment_id = s.segment_id AND c.time >= ?
            ) DESC, s.updated_at DESC, s.segment_id ASC
            LIMIT 1
        )
        SELECT r.time, r.open, r.high, r.low, r.close, r.volume
        FROM ohlcv_rows AS r
        JOIN best ON best.segment_id = r.segment_id
        WHERE r.time >= ?
        ORDER BY r.time{limit_sql}
    """
    raw = connection.execute(query, parameters).fetchall()
    return [canonical_row(row) for row in raw]
