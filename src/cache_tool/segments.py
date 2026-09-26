"""所有片段类型共享的实际业务时间交集选择，不以相邻或范围相交连接。"""


def select_segment(
    connection, series_key: str, kind: str, table: str, time_column: str, incoming: str
):
    # 表名和列名仅来自本包的固定调用点，值仍通过参数绑定。
    overlapping = connection.execute(
        f"""
        SELECT s.segment_id,s.covered_from,s.row_count FROM cache_segments s
        WHERE s.series_key=? AND s.data_kind=? AND EXISTS (
            SELECT 1 FROM {table} r JOIN {incoming} i ON i.{time_column}=r.{time_column}
            WHERE r.segment_id=s.segment_id)
        ORDER BY s.row_count DESC,s.segment_id ASC
    """,
        [series_key, kind],
    ).fetchall()
    if not overlapping:
        return (
            connection.execute("SELECT nextval('cache_segment_id_seq')").fetchone()[0],
            [],
            [],
        )
    return (
        overlapping[0][0],
        [row[0] for row in overlapping[1:]],
        [row[1] for row in overlapping],
    )
