def upgrade_metadata(connection) -> None:
    connection.execute("""
        CREATE TABLE calendar_rows(segment_id BIGINT, date DATE, trading BOOLEAN NOT NULL,
                                  PRIMARY KEY(segment_id,date))
    """)
    connection.execute("""
        CREATE TABLE mapping_rows(segment_id BIGINT,symbol VARCHAR NOT NULL,
            trading_date DATE,underlying_symbol VARCHAR NOT NULL,roll_date DATE NOT NULL,
            PRIMARY KEY(segment_id,trading_date))
    """)
    connection.execute("""
        CREATE TABLE mapping_context(segment_id BIGINT,roll_date DATE,
            underlying_symbol VARCHAR NOT NULL,old_symbol VARCHAR,
            PRIMARY KEY(segment_id,roll_date,underlying_symbol))
    """)
    connection.execute("""
        CREATE TABLE metadata_source_facts(series_key VARCHAR PRIMARY KEY, kind VARCHAR NOT NULL,
            digest VARCHAR NOT NULL,calendar_digest VARCHAR,holiday_last DATE,
            valid_from DATE,valid_to DATE,server_time BIGINT NOT NULL,
            verified_date DATE,underlying_symbol VARCHAR,reference_time BIGINT)
    """)
