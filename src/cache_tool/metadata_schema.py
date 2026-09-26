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


def upgrade_mapping_links(connection) -> None:
    columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info('mapping_rows')").fetchall()
    }
    if "roll_date" in columns:
        return
    # 旧版没有保存逐行节点证据，不能按当前上下文猜测历史关联。
    # 保留旧行及上下文；NULL 关联使完整读取 miss，正常源查询后再原子补齐。
    connection.execute("ALTER TABLE mapping_rows ADD COLUMN roll_date DATE")
    connection.execute("""
        CREATE TABLE mapping_context_v5(segment_id BIGINT,roll_date DATE,
            underlying_symbol VARCHAR NOT NULL,old_symbol VARCHAR,
            PRIMARY KEY(segment_id,roll_date,underlying_symbol))
    """)
    connection.execute("INSERT INTO mapping_context_v5 SELECT * FROM mapping_context")
    connection.execute("DROP TABLE mapping_context")
    connection.execute("ALTER TABLE mapping_context_v5 RENAME TO mapping_context")
