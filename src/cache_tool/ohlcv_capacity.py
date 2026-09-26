from src.domain_errors import CacheCapacityExceeded


class OhlcvCapacity:
    max_rows_per_series: int
    max_rows_total: int

    def _refresh_metadata(self, connection) -> None:
        connection.execute("""
            UPDATE cache_segments AS s SET
                covered_from=CASE WHEN a.first_time>s.first_time THEN a.first_time ELSE s.covered_from END,
                first_time=a.first_time, last_time=a.last_time, row_count=a.row_count,
                updated_at=CASE WHEN a.first_time<>s.first_time OR a.last_time<>s.last_time
                    OR a.row_count<>s.row_count THEN CURRENT_TIMESTAMP ELSE s.updated_at END
            FROM (SELECT segment_id, MIN(time) first_time, MAX(time) last_time,
                         COUNT(*) row_count FROM ohlcv_rows GROUP BY segment_id) AS a
            WHERE s.segment_id=a.segment_id AND s.data_kind='ohlcv'
        """)
        connection.execute("""
            DELETE FROM cache_segments AS s
            WHERE s.data_kind='ohlcv' AND NOT EXISTS (SELECT 1 FROM ohlcv_rows AS r WHERE r.segment_id=s.segment_id)
        """)

    def _enforce_capacity(self, connection, series_key: str) -> None:
        series_count = connection.execute(
            """
            SELECT COUNT(*) FROM (SELECT r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.data_kind='ohlcv' AND s.series_key=? GROUP BY r.time)
        """,
            [series_key],
        ).fetchone()[0]
        if series_count > self.max_rows_per_series:
            target = int(self.max_rows_per_series * 0.9)
            self._evict_series(connection, series_key, series_count - target)
        total_count = connection.execute("""
            SELECT COUNT(*) FROM (SELECT s.series_key, r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.data_kind='ohlcv' GROUP BY s.series_key, r.time)
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
            JOIN cache_segments s USING(segment_id) WHERE s.data_kind='ohlcv' AND s.series_key=?
            GROUP BY r.time ORDER BY r.time LIMIT ?
        """,
            [series_key, count],
        )
        connection.execute(
            """
            DELETE FROM ohlcv_rows AS r USING cache_segments AS s
            WHERE s.data_kind='ohlcv' AND r.segment_id=s.segment_id AND s.series_key=?
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
            WHERE s.data_kind='ohlcv'
            GROUP BY s.series_key, r.time
            ORDER BY MIN(CASE WHEN s.time_unit='ms' THEN CAST(r.time AS HUGEINT)*1000000
                        ELSE CAST(r.time AS HUGEINT) END), MIN(r.segment_id) LIMIT ?
        """,
            [count],
        )
        connection.execute("""
            DELETE FROM ohlcv_rows AS r USING cache_segments AS s
            WHERE s.data_kind='ohlcv' AND r.segment_id=s.segment_id AND EXISTS (
                SELECT 1 FROM evict_identities e
                WHERE e.series_key=s.series_key AND e.time=r.time)
        """)

    def _count_series(self, connection, series_key: str) -> int:
        return connection.execute(
            """
            SELECT COUNT(*) FROM (SELECT r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.data_kind='ohlcv' AND s.series_key=? GROUP BY r.time)
        """,
            [series_key],
        ).fetchone()[0]

    def _count_total(self, connection) -> int:
        return connection.execute("""
            SELECT COUNT(*) FROM (SELECT s.series_key,r.time FROM ohlcv_rows r
            JOIN cache_segments s USING(segment_id) WHERE s.data_kind='ohlcv' GROUP BY s.series_key,r.time)
        """).fetchone()[0]
