import threading
from pathlib import Path
from typing import Any

import duckdb

from src.cache_tool.models import OhlcvResult, OhlcvRow, canonical_row
from src.domain_errors import CacheCapacityExceeded

SCHEMA_VERSION = "1"


class DuckDbOhlcvCache:
    _locks_guard = threading.Lock()
    _path_locks: dict[str, threading.Lock] = {}

    def __init__(
        self,
        database_path: str | Path,
        max_rows_per_series: int,
        max_rows_total: int,
    ) -> None:
        if not 100_000 < max_rows_per_series <= max_rows_total:
            raise ValueError("cache limits must satisfy 100000 < per-series <= total")
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.max_rows_per_series = max_rows_per_series
        self.max_rows_total = max_rows_total
        self._local = threading.local()
        path_key = str(self.database_path)
        with self._locks_guard:
            self._write_lock = self._path_locks.setdefault(path_key, threading.Lock())
        with self._write_lock:
            self._ensure_schema(self._connection())

    def read_best_prefix(
        self,
        series_key: str,
        since: int,
        max_rows: int | None,
    ) -> list[OhlcvRow]:
        if max_rows is not None and max_rows <= 0:
            return []
        limit_sql = "" if max_rows is None else " LIMIT ?"
        parameters: list[Any] = [series_key, since, since, since, since]
        if max_rows is not None:
            parameters.append(max_rows)
        query = f"""
            WITH best AS (
                SELECT s.segment_id
                FROM cache_segments AS s
                WHERE s.series_key = ?
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
        raw = self._connection().execute(query, parameters).fetchall()
        return [canonical_row(row) for row in raw]

    def write_segment(
        self,
        series_key: str,
        result: OhlcvResult,
        verified_covered_from: int | None,
    ) -> None:
        source_rows = (
            result.rows if result.last_bar_completion_confirmed else result.rows[:-1]
        )
        rows = self._valid_rows(source_rows)
        if not rows:
            return
        first_time = rows[0][0]
        covered_from = (
            first_time if verified_covered_from is None else verified_covered_from
        )
        if covered_from > first_time:
            raise ValueError("verified_covered_from must not exceed first row time")

        with self._write_lock:
            connection = self._connection()
            connection.execute("BEGIN TRANSACTION")
            try:
                self._load_incoming(connection, rows)
                segment_id, absorbed, existing_coverage = self._select_segment(
                    connection, series_key
                )
                coverage = min([covered_from, *existing_coverage])
                self._merge_into_segment(
                    connection, series_key, segment_id, absorbed, coverage, rows
                )
                try:
                    self._enforce_capacity(connection, series_key)
                except CacheCapacityExceeded:
                    raise
                except Exception as exc:
                    raise CacheCapacityExceeded("cache eviction failed") from exc
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def _connection(self):
        connection = getattr(self._local, "connection", None)
        if connection is None:
            connection = duckdb.connect(str(self.database_path))
            self._local.connection = connection
        return connection

    def _ensure_schema(self, connection) -> None:
        connection.execute("CREATE SEQUENCE IF NOT EXISTS cache_segment_id_seq START 1")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
        )
        connection.execute(
            "INSERT OR IGNORE INTO cache_meta VALUES ('schema_version', ?)",
            [SCHEMA_VERSION],
        )
        version = connection.execute(
            "SELECT value FROM cache_meta WHERE key='schema_version'"
        ).fetchone()[0]
        if version != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported cache schema version: {version}")
        connection.execute("""
            CREATE TABLE IF NOT EXISTS cache_segments (
                segment_id BIGINT PRIMARY KEY, series_key VARCHAR NOT NULL,
                covered_from BIGINT NOT NULL, first_time BIGINT NOT NULL,
                last_time BIGINT NOT NULL, row_count BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_rows (
                segment_id BIGINT NOT NULL, time BIGINT NOT NULL,
                open DOUBLE NOT NULL, high DOUBLE NOT NULL, low DOUBLE NOT NULL,
                close DOUBLE NOT NULL, volume DOUBLE NOT NULL,
                PRIMARY KEY (segment_id, time)
            )
        """)
        connection.execute(
            "CREATE INDEX IF NOT EXISTS cache_segments_series ON cache_segments(series_key)"
        )

    def _valid_rows(self, rows: list[OhlcvRow]) -> list[OhlcvRow]:
        valid: dict[int, OhlcvRow] = {}
        for values in rows:
            try:
                row = canonical_row(values)
            except (TypeError, ValueError, OverflowError):
                return []
            valid[row[0]] = row
        return [valid[timestamp] for timestamp in sorted(valid)]

    def _load_incoming(self, connection, rows: list[OhlcvRow]) -> None:
        connection.execute("""
            CREATE TEMP TABLE IF NOT EXISTS incoming_ohlcv (
                time BIGINT PRIMARY KEY, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE, volume DOUBLE
            )
        """)
        connection.execute("DELETE FROM incoming_ohlcv")
        connection.executemany(
            "INSERT INTO incoming_ohlcv VALUES (?, ?, ?, ?, ?, ?)", rows
        )

    def _select_segment(self, connection, series_key: str):
        overlapping = connection.execute(
            """
            SELECT s.segment_id, s.covered_from, s.row_count
            FROM cache_segments AS s
            WHERE s.series_key = ? AND EXISTS (
                SELECT 1 FROM ohlcv_rows AS r JOIN incoming_ohlcv AS i ON i.time=r.time
                WHERE r.segment_id=s.segment_id
            )
            ORDER BY s.row_count DESC, s.segment_id ASC
        """,
            [series_key],
        ).fetchall()
        if not overlapping:
            segment_id = connection.execute(
                "SELECT nextval('cache_segment_id_seq')"
            ).fetchone()[0]
            return segment_id, [], []
        segment_id = overlapping[0][0]
        absorbed = [row[0] for row in overlapping[1:]]
        coverage = [row[1] for row in overlapping]
        return segment_id, absorbed, coverage

    def _merge_into_segment(
        self,
        connection,
        series_key: str,
        segment_id: int,
        absorbed: list[int],
        covered_from: int,
        rows: list[OhlcvRow],
    ) -> None:
        existing = connection.execute(
            "SELECT 1 FROM cache_segments WHERE segment_id=?", [segment_id]
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO cache_segments VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                [
                    segment_id,
                    series_key,
                    covered_from,
                    rows[0][0],
                    rows[-1][0],
                    len(rows),
                ],
            )
        if absorbed:
            marks = ",".join("?" for _ in absorbed)
            connection.execute(
                f"""
                INSERT OR IGNORE INTO ohlcv_rows
                SELECT ?, time, open, high, low, close, volume FROM ohlcv_rows
                WHERE segment_id IN ({marks})
            """,
                [segment_id, *absorbed],
            )
        connection.execute(
            """
            INSERT INTO ohlcv_rows
            SELECT ?, time, open, high, low, close, volume FROM incoming_ohlcv
            ON CONFLICT (segment_id, time) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume
        """,
            [segment_id],
        )
        if absorbed:
            marks = ",".join("?" for _ in absorbed)
            connection.execute(
                f"DELETE FROM ohlcv_rows WHERE segment_id IN ({marks})", absorbed
            )
            connection.execute(
                f"DELETE FROM cache_segments WHERE segment_id IN ({marks})", absorbed
            )
        connection.execute(
            """
            UPDATE cache_segments SET covered_from=?, updated_at=CURRENT_TIMESTAMP
            WHERE segment_id=?
        """,
            [covered_from, segment_id],
        )
        self._refresh_metadata(connection)

    def _refresh_metadata(self, connection) -> None:
        connection.execute("""
            UPDATE cache_segments AS s SET
                covered_from=CASE WHEN a.first_time>s.first_time THEN a.first_time ELSE s.covered_from END,
                first_time=a.first_time, last_time=a.last_time, row_count=a.row_count,
                updated_at=CASE WHEN a.first_time<>s.first_time OR a.last_time<>s.last_time
                    OR a.row_count<>s.row_count THEN CURRENT_TIMESTAMP ELSE s.updated_at END
            FROM (SELECT segment_id, MIN(time) first_time, MAX(time) last_time,
                         COUNT(*) row_count FROM ohlcv_rows GROUP BY segment_id) AS a
            WHERE s.segment_id=a.segment_id
        """)
        connection.execute("""
            DELETE FROM cache_segments AS s
            WHERE NOT EXISTS (SELECT 1 FROM ohlcv_rows AS r WHERE r.segment_id=s.segment_id)
        """)

    def _enforce_capacity(self, connection, series_key: str) -> None:
        series_count = connection.execute(
            """
            SELECT COUNT(*) FROM (SELECT r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.series_key=? GROUP BY r.time)
        """,
            [series_key],
        ).fetchone()[0]
        if series_count > self.max_rows_per_series:
            target = int(self.max_rows_per_series * 0.9)
            self._evict_series(connection, series_key, series_count - target)
        total_count = connection.execute("""
            SELECT COUNT(*) FROM (SELECT s.series_key, r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) GROUP BY s.series_key, r.time)
        """).fetchone()[0]
        if total_count > self.max_rows_total:
            target = int(self.max_rows_total * 0.9)
            self._evict_global(connection, total_count - target)
        self._refresh_metadata(connection)
        if self._count_series(connection, series_key) > self.max_rows_per_series:
            raise CacheCapacityExceeded("per-series eviction did not reach its limit")
        if self._count_total(connection) > self.max_rows_total:
            raise CacheCapacityExceeded("global eviction did not reach its limit")

    def _evict_series(self, connection, series_key: str, count: int) -> None:
        connection.execute(
            "CREATE TEMP TABLE IF NOT EXISTS evict_times(time BIGINT PRIMARY KEY)"
        )
        connection.execute("DELETE FROM evict_times")
        connection.execute(
            """
            INSERT INTO evict_times SELECT r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.series_key=?
            GROUP BY r.time ORDER BY r.time LIMIT ?
        """,
            [series_key, count],
        )
        connection.execute(
            """
            DELETE FROM ohlcv_rows AS r USING cache_segments AS s
            WHERE r.segment_id=s.segment_id AND s.series_key=?
              AND r.time IN (SELECT time FROM evict_times)
        """,
            [series_key],
        )

    def _evict_global(self, connection, count: int) -> None:
        connection.execute("""
            CREATE TEMP TABLE IF NOT EXISTS evict_identities(
                series_key VARCHAR, time BIGINT, PRIMARY KEY(series_key,time))
        """)
        connection.execute("DELETE FROM evict_identities")
        connection.execute(
            """
            INSERT INTO evict_identities
            SELECT s.series_key, r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id)
            GROUP BY s.series_key, r.time ORDER BY r.time, MIN(r.segment_id) LIMIT ?
        """,
            [count],
        )
        connection.execute("""
            DELETE FROM ohlcv_rows AS r USING cache_segments AS s
            WHERE r.segment_id=s.segment_id AND EXISTS (
                SELECT 1 FROM evict_identities e
                WHERE e.series_key=s.series_key AND e.time=r.time)
        """)

    def _count_series(self, connection, series_key: str) -> int:
        return connection.execute(
            """
            SELECT COUNT(*) FROM (SELECT r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.series_key=? GROUP BY r.time)
        """,
            [series_key],
        ).fetchone()[0]

    def _count_total(self, connection) -> int:
        return connection.execute("""
            SELECT COUNT(*) FROM (SELECT s.series_key,r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) GROUP BY s.series_key,r.time)
        """).fetchone()[0]
