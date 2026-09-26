import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from src.cache_tool import metadata_store, ohlcv_readers, transition_store
from src.cache_tool.duckdb_schema import ensure_schema
from src.cache_tool.metadata_models import CalendarSourceResult, MappingSourceResult
from src.cache_tool.models import (
    OhlcvResult,
    OhlcvRow,
    TqOhlcvBatch,
    TqOhlcvSeries,
    canonical_row,
    eligible_rows,
    tq_storage_rows,
)
from src.cache_tool.ohlcv_capacity import OhlcvCapacity
from src.cache_tool.segments import select_segment
from src.cache_tool.transition_store import TransitionIdentity
from src.domain_errors import CacheCapacityExceeded


class DuckDbOhlcvCache(OhlcvCapacity):
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
        self._connections_guard = threading.Lock()
        self._connections: list[Any] = []
        self._lifecycle = threading.Condition()
        self._active_readers = 0
        self._closing = False
        self._closed = False
        path_key = str(self.database_path)
        with self._locks_guard:
            self._write_lock = self._path_locks.setdefault(path_key, threading.Lock())
        with self._write_lock:
            try:
                ensure_schema(self._connection())
            except BaseException:
                for connection in self._connections:
                    connection.close()
                self._closed = True
                raise

    def read_best_prefix(
        self,
        series_key: str,
        since: int,
        max_rows: int | None,
    ) -> list[OhlcvRow]:
        with self._reader_scope():
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
            raw = self._connection().execute(query, parameters).fetchall()
            return [canonical_row(row) for row in raw]

    def write_segment(
        self,
        series_key: str,
        result: OhlcvResult,
        verified_covered_from: int | None,
    ) -> None:
        self._require_open()
        rows = self._valid_rows(
            eligible_rows(result.rows, result.last_bar_completion_confirmed)
        )
        self._write_rows(
            series_key,
            [(*row, None, None, None) for row in rows],
            verified_covered_from,
            "ms",
        )

    def write_tq_segment(self, series: TqOhlcvSeries, batch: TqOhlcvBatch) -> None:
        self._require_open()
        self._write_rows(series.key, tq_storage_rows(series, batch), None, "ns")

    def read_contiguous_before(self, series_key: str, end_time: int, max_rows: int):
        with self._reader_scope():
            return ohlcv_readers.read_before(
                self._connection(), series_key, end_time, max_rows
            )

    def read_connected_history(
        self, series_key: str, batch: OhlcvResult | TqOhlcvBatch, max_rows: int
    ):
        with self._reader_scope():
            return ohlcv_readers.read_connected(
                self._connection(), series_key, batch, max_rows
            )

    def read_latest_summary(self, series_key: str):
        with self._reader_scope():
            return ohlcv_readers.read_summary(self._connection(), series_key)

    def _write_rows(
        self,
        series_key: str,
        rows: list[tuple],
        verified_covered_from: int | None,
        time_unit: str,
    ) -> None:
        if not rows:
            return
        first_time = rows[0][0]
        covered_from = (
            first_time if verified_covered_from is None else verified_covered_from
        )
        if covered_from > first_time:
            raise ValueError("verified_covered_from must not exceed first row time")

        with self._transaction() as connection:
            conflict = connection.execute(
                "SELECT 1 FROM cache_segments WHERE series_key=? AND (data_kind<>'ohlcv' OR time_unit<>?) LIMIT 1",
                [series_key, time_unit],
            ).fetchone()
            if conflict:
                raise ValueError("series kind/unit conflict")
            self._load_incoming(connection, rows)
            segment_id, absorbed, existing_coverage = self._select_segment(
                connection, series_key
            )
            coverage = min([covered_from, *existing_coverage])
            self._merge_into_segment(
                connection,
                series_key,
                segment_id,
                absorbed,
                coverage,
                rows,
                time_unit,
            )
            try:
                self._enforce_capacity(connection, series_key)
            except CacheCapacityExceeded:
                raise
            except Exception as exc:
                raise CacheCapacityExceeded("cache eviction failed") from exc

    @contextmanager
    def _transaction(self):
        with self._write_lock:
            connection = self._connection()
            connection.execute("BEGIN TRANSACTION")
            try:
                yield connection
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    @contextmanager
    def _read_transaction(self):
        with self._reader_scope():
            connection = self._connection()
            connection.execute("BEGIN TRANSACTION")
            try:
                yield connection
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    def read_calendar_range(self, start: date, end: date):
        with self._read_transaction() as connection:
            return metadata_store.read_calendar(connection, start, end)

    def submit_calendar(self, result: CalendarSourceResult) -> None:
        with self._transaction() as connection:
            metadata_store.submit_calendar(connection, result)

    def read_mapping_range(self, symbol: str, dates: list[date], max_date: date):
        with self._read_transaction() as connection:
            return metadata_store.read_mapping(connection, symbol, dates, max_date)

    def submit_mapping(self, result: MappingSourceResult) -> None:
        with self._transaction() as connection:
            metadata_store.submit_mapping(connection, result)

    def read_matching_mapping(self, result: MappingSourceResult):
        with self._read_transaction() as connection:
            return metadata_store.read_matching_mapping(connection, result)

    def read_metadata_facts(self, kind: str, symbol: str | None = None):
        with self._reader_scope():
            return metadata_store.read_facts(self._connection(), kind, symbol)

    def read_transition_prefix(self, identity: TransitionIdentity, count: int):
        with self._reader_scope():
            return transition_store.read_prefix(self._connection(), identity, count)

    def submit_transition_window(
        self, identity: TransitionIdentity, target_count: int, batch: TqOhlcvBatch
    ) -> None:
        with self._transaction() as connection:
            transition_store.submit(connection, identity, target_count, batch)

    def close(self) -> None:
        with self._write_lock:
            with self._lifecycle:
                if self._closed:
                    return
                self._closing = True
                self._lifecycle.wait_for(lambda: self._active_readers == 0)
                self._closed = True
                self._closing = False
            with self._connections_guard:
                connections = self._connections
                self._connections = []
            for connection in connections:
                connection.close()
            self._local = threading.local()

    @contextmanager
    def _reader_scope(self) -> Iterator[None]:
        with self._lifecycle:
            if self._closing or self._closed:
                raise RuntimeError("DuckDbOhlcvCache is closed")
            self._active_readers += 1
        try:
            yield
        finally:
            with self._lifecycle:
                self._active_readers -= 1
                if self._active_readers == 0:
                    self._lifecycle.notify_all()

    def _require_open(self) -> None:
        with self._lifecycle:
            if self._closing or self._closed:
                raise RuntimeError("DuckDbOhlcvCache is closed")

    def _connection(self):
        with self._lifecycle:
            if self._closed:
                raise RuntimeError("DuckDbOhlcvCache is closed")
        connection = getattr(self._local, "connection", None)
        if connection is None:
            with self._connections_guard:
                connection = duckdb.connect(str(self.database_path))
                self._connections.append(connection)
            self._local.connection = connection
        return connection

    def _valid_rows(self, rows: list[OhlcvRow]) -> list[OhlcvRow]:
        valid: dict[int, OhlcvRow] = {}
        for values in rows:
            try:
                row = canonical_row(values)
            except (TypeError, ValueError, OverflowError):
                return []
            valid[row[0]] = row
        return [valid[timestamp] for timestamp in sorted(valid)]

    def _load_incoming(self, connection, rows: list[tuple]) -> None:
        connection.execute("""
            CREATE TEMP TABLE IF NOT EXISTS incoming_ohlcv (
                time BIGINT PRIMARY KEY, open DOUBLE, high DOUBLE,
                low DOUBLE, close DOUBLE, volume DOUBLE,
                sdk_id BIGINT, open_oi DOUBLE, close_oi DOUBLE
            )
        """)
        connection.execute("DELETE FROM incoming_ohlcv")
        # 按列一次提交，保留纳秒整数精度，避免一万根逐行执行 SQL。
        connection.execute(
            """INSERT INTO incoming_ohlcv SELECT
                unnest(?::BIGINT[]), unnest(?::DOUBLE[]), unnest(?::DOUBLE[]),
                unnest(?::DOUBLE[]), unnest(?::DOUBLE[]), unnest(?::DOUBLE[]),
                unnest(?::BIGINT[]), unnest(?::DOUBLE[]), unnest(?::DOUBLE[])""",
            list(zip(*rows)),
        )

    def _select_segment(self, connection, series_key: str):
        return select_segment(
            connection, series_key, "ohlcv", "ohlcv_rows", "time", "incoming_ohlcv"
        )

    def _merge_into_segment(
        self,
        connection,
        series_key: str,
        segment_id: int,
        absorbed: list[int],
        covered_from: int,
        rows: list[tuple],
        time_unit: str,
    ) -> None:
        existing = connection.execute(
            "SELECT 1 FROM cache_segments WHERE segment_id=?", [segment_id]
        ).fetchone()
        if existing is None:
            connection.execute(
                "INSERT INTO cache_segments VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'ohlcv', ?)",
                [
                    segment_id,
                    series_key,
                    covered_from,
                    rows[0][0],
                    rows[-1][0],
                    len(rows),
                    time_unit,
                ],
            )
        if absorbed:
            marks = ",".join("?" for _ in absorbed)
            connection.execute(
                f"""
                INSERT OR IGNORE INTO ohlcv_rows
                SELECT ?, time, open, high, low, close, volume, sdk_id, open_oi, close_oi FROM ohlcv_rows
                WHERE segment_id IN ({marks})
            """,
                [segment_id, *absorbed],
            )
        connection.execute(
            """
            INSERT INTO ohlcv_rows
            SELECT ?, time, open, high, low, close, volume, sdk_id, open_oi, close_oi FROM incoming_ohlcv
            ON CONFLICT (segment_id, time) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                sdk_id=excluded.sdk_id, open_oi=excluded.open_oi, close_oi=excluded.close_oi
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
