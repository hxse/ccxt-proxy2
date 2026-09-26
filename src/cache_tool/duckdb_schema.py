from src.cache_tool.metadata_schema import upgrade_metadata

SCHEMA_VERSION = "3"


def _ensure_schema(connection) -> None:
    connection.execute("CREATE SEQUENCE IF NOT EXISTS cache_segment_id_seq START 1")
    connection.execute(
        "CREATE TABLE IF NOT EXISTS cache_meta (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)"
    )
    connection.execute(
        "INSERT OR IGNORE INTO cache_meta VALUES ('schema_version', ?)",
        ["1"],
    )
    version = connection.execute(
        "SELECT value FROM cache_meta WHERE key='schema_version'"
    ).fetchone()[0]
    if version not in ("1", "2", SCHEMA_VERSION):
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

    if version == "1":
        connection.execute(
            "ALTER TABLE cache_segments ADD COLUMN data_kind VARCHAR DEFAULT 'ohlcv'"
        )
        connection.execute(
            "ALTER TABLE cache_segments ADD COLUMN time_unit VARCHAR DEFAULT 'ms'"
        )
        connection.execute("ALTER TABLE ohlcv_rows ADD COLUMN sdk_id BIGINT")
        connection.execute("ALTER TABLE ohlcv_rows ADD COLUMN open_oi DOUBLE")
        connection.execute("ALTER TABLE ohlcv_rows ADD COLUMN close_oi DOUBLE")
        version = "2"
    if version == "2":
        upgrade_metadata(connection)
    connection.execute(
        "UPDATE cache_meta SET value=? WHERE key='schema_version'", [SCHEMA_VERSION]
    )


def ensure_schema(connection) -> None:
    connection.execute("BEGIN TRANSACTION")
    try:
        _ensure_schema(connection)
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
