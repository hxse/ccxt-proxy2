SCHEMA_VERSION = "1"


def ensure_schema(connection) -> None:
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
