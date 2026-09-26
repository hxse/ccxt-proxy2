"""一次稳定目标窗口的整窗存储，N 不属于身份，不与普通片段拼长。"""

import json
from dataclasses import dataclass, replace
from datetime import date

from src.cache_tool.models import (
    TqOhlcvBatch,
    TqOhlcvSeries,
    eligible_rows,
    tq_storage_rows,
)


@dataclass(frozen=True, slots=True)
class TransitionIdentity:
    symbol: str
    roll_date: date
    new_symbol: str
    old_symbol: str
    duration_seconds: int

    @property
    def key(self) -> str:
        return json.dumps(
            {
                "provider": "tq",
                "mode": "live",
                "symbol": self.symbol,
                "roll_date": self.roll_date.isoformat(),
                "new_symbol": self.new_symbol,
                "old_symbol": self.old_symbol,
                "timeframe": f"{self.duration_seconds}s",
                "variant": "default",
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def upgrade_transition(connection) -> None:
    connection.execute("""
        CREATE TABLE transition_windows(segment_id BIGINT PRIMARY KEY,series_key VARCHAR UNIQUE,
            symbol VARCHAR,roll_date DATE,new_symbol VARCHAR,old_symbol VARCHAR,
            duration_seconds BIGINT,variant VARCHAR)
    """)
    connection.execute("""
        CREATE TABLE transition_rows(segment_id BIGINT,datetime BIGINT,
            old_open DOUBLE NOT NULL,old_high DOUBLE NOT NULL,old_low DOUBLE NOT NULL,
            old_close DOUBLE NOT NULL,old_volume DOUBLE NOT NULL,
            PRIMARY KEY(segment_id,datetime))
    """)


def read_prefix(connection, identity: TransitionIdentity, count: int):
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise ValueError("positive transition count required")
    rows = connection.execute(
        """
        SELECT r.datetime,r.old_open,r.old_high,r.old_low,r.old_close,r.old_volume
        FROM transition_rows r JOIN transition_windows w USING(segment_id)
        JOIN cache_segments s USING(segment_id)
        WHERE w.series_key=? AND s.data_kind='transition' AND s.time_unit='ns' AND s.row_count>=?
        ORDER BY r.datetime LIMIT ?
    """,
        [identity.key, count, count],
    ).fetchall()
    if len(rows) != count:
        return None
    return [
        dict(
            zip(
                (
                    "datetime",
                    "old_open",
                    "old_high",
                    "old_low",
                    "old_close",
                    "old_volume",
                ),
                row,
                strict=True,
            )
        )
        for row in rows
    ]


def submit(
    connection, identity: TransitionIdentity, target_count: int, batch: TqOhlcvBatch
) -> None:
    if (
        isinstance(target_count, bool)
        or not isinstance(target_count, int)
        or target_count <= 0
        or len(batch.records) != target_count + 1
        or batch.last_bar_completion_confirmed is not False
    ):
        raise ValueError(
            "transition needs one complete N+1 window with unknown last row"
        )
    series = TqOhlcvSeries(identity.old_symbol, identity.duration_seconds)
    # 连证明行也必须有效，再使用普通行情的唯一尾根筛选器。
    complete = tq_storage_rows(
        series, replace(batch, last_bar_completion_confirmed=True)
    )
    rows = eligible_rows(complete, batch.last_bar_completion_confirmed)
    if len(rows) != target_count:
        raise ValueError("invalid transition window")
    existing = connection.execute(
        """
        SELECT w.segment_id,s.row_count FROM transition_windows w
        JOIN cache_segments s USING(segment_id) WHERE w.series_key=?
    """,
        [identity.key],
    ).fetchone()
    if existing and existing[1] > target_count:
        return
    if existing:
        segment = existing[0]
        connection.execute("DELETE FROM transition_rows WHERE segment_id=?", [segment])
    else:
        segment = connection.execute(
            "SELECT nextval('cache_segment_id_seq')"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO transition_windows VALUES (?,?,?,?,?,?,?,'default')",
            [
                segment,
                identity.key,
                identity.symbol,
                identity.roll_date,
                identity.new_symbol,
                identity.old_symbol,
                identity.duration_seconds,
            ],
        )
    connection.execute(
        """
        INSERT INTO cache_segments VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'transition','ns')
        ON CONFLICT(segment_id) DO UPDATE SET covered_from=excluded.covered_from,
            first_time=excluded.first_time,last_time=excluded.last_time,row_count=excluded.row_count,
            updated_at=excluded.updated_at
    """,
        [segment, identity.key, rows[0][0], rows[0][0], rows[-1][0], target_count],
    )
    connection.executemany(
        "INSERT INTO transition_rows VALUES (?,?,?,?,?,?,?)",
        [(segment, *row[:6]) for row in rows],
    )
