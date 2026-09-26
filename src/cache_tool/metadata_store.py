"""类型化日期存储；事务及 reader scope 由缓存外观持有。"""

from datetime import date

from src.cache_tool.metadata_models import (
    CachedMapping,
    CalendarDay,
    CalendarFacts,
    CalendarSourceResult,
    MappingDay,
    MappingFacts,
    MappingNode,
    MappingSourceResult,
    mapping_nodes_for,
    metadata_key,
    validate_calendar,
)
from src.cache_tool.segments import select_segment


def read_facts(connection, kind: str, symbol: str | None = None):
    row = connection.execute(
        "SELECT * FROM metadata_source_facts WHERE series_key=? AND kind=?",
        [metadata_key(kind, symbol), kind],
    ).fetchone()
    if row is None:
        return None
    if kind == "calendar":
        return CalendarFacts(row[4], row[5], row[6], row[2], row[7])
    return MappingFacts(row[2], row[3], row[7], row[8], row[9], row[10])


def _segment(connection, key: str, kind: str, start: date, end: date):
    return connection.execute(
        """
        SELECT segment_id FROM cache_segments
        WHERE series_key=? AND data_kind=? AND time_unit='date'
          AND covered_from<=? AND last_time>=?
        ORDER BY row_count DESC,updated_at DESC,segment_id ASC LIMIT 1
    """,
        [key, kind, start.toordinal(), end.toordinal()],
    ).fetchone()


def read_calendar(connection, start: date, end: date) -> CalendarSourceResult | None:
    segment = _segment(connection, metadata_key("calendar"), "calendar", start, end)
    facts = read_facts(connection, "calendar")
    if not segment or not isinstance(facts, CalendarFacts):
        return None
    rows = connection.execute(
        "SELECT date,trading FROM calendar_rows WHERE segment_id=? AND date BETWEEN ? AND ? ORDER BY date",
        [segment[0], start, end],
    ).fetchall()
    records = [CalendarDay(*row) for row in rows]
    try:
        validate_calendar(records, start, end)
    except ValueError:
        return None
    return CalendarSourceResult(records, facts)


def read_mapping(
    connection, symbol: str, dates: list[date], max_date: date
) -> CachedMapping | None:
    if not dates:
        return CachedMapping([], [])
    if dates != sorted(set(dates)) or dates[-1] > max_date:
        raise ValueError("mapping dates exceed verified boundary or are unordered")
    segment = _segment(
        connection,
        metadata_key("main_mapping", symbol),
        "main_mapping",
        dates[0],
        dates[-1],
    )
    if not segment:
        return None
    rows = connection.execute(
        """SELECT r.trading_date,r.underlying_symbol,c.roll_date,c.old_symbol
        FROM mapping_rows r JOIN mapping_context c
          ON r.segment_id=c.segment_id AND r.roll_date=c.roll_date
          AND r.underlying_symbol=c.underlying_symbol
        WHERE r.segment_id=? AND trading_date BETWEEN ? AND ? ORDER BY trading_date""",
        [segment[0], dates[0], dates[-1]],
    ).fetchall()
    if [row[0] for row in rows] != dates:
        return None
    records = [MappingDay(row[0], row[1]) for row in rows]
    linked = [MappingNode(row[2], row[1], row[3]) for row in rows]
    # 历史修订可能移动或撤销节点。必须使用每条日记录提交时的确切关联；
    # 若部分修订与尚未重新覆盖的旧行矛盾，整体 miss，交官方完整查询修复。
    if any(
        a.date > b.date
        or (a.date == b.date and a != b)
        or (a.date < b.date and b.old_symbol != a.underlying_symbol)
        for a, b in zip(linked, linked[1:])
    ):
        return None
    try:
        nodes = mapping_nodes_for(records, list(dict.fromkeys(linked)))
    except ValueError:
        return None
    return CachedMapping(records, nodes)


def read_matching_mapping(connection, result: MappingSourceResult):
    """在同一读事务中核对本次源、实际覆盖及精确节点，不借全局摘要信任旧片段。"""
    facts = read_facts(connection, "main_mapping", result.symbol)
    if not isinstance(facts, MappingFacts) or (
        facts.digest != result.facts.digest
        or facts.calendar_digest != result.facts.calendar_digest
    ):
        return None
    day = result.facts.verified_date
    current = read_mapping(connection, result.symbol, [day], day)
    history = read_mapping(
        connection, result.symbol, [row.date for row in result.records], day
    )
    if (
        current is None
        or current.records != [MappingDay(day, result.facts.underlying_symbol)]
        or current.nodes != [result.verification_node]
        or history is None
        or history.records != result.records
        or history.nodes != result.nodes
    ):
        return None
    return history


def _merge_dates(
    connection, key: str, kind: str, records, symbol: str | None = None, nodes=None
):
    conflict = connection.execute(
        "SELECT 1 FROM cache_segments WHERE series_key=? AND (data_kind<>? OR time_unit<>'date') LIMIT 1",
        [key, kind],
    ).fetchone()
    if conflict:
        raise ValueError("series kind/unit conflict")
    calendar = kind == "calendar"
    table, column = (
        ("calendar_rows", "date") if calendar else ("mapping_rows", "trading_date")
    )
    incoming = "incoming_calendar" if calendar else "incoming_mapping"
    columns = (
        "date,trading"
        if calendar
        else "trading_date,symbol,underlying_symbol,roll_date"
    )
    definition = (
        "date DATE,trading BOOLEAN"
        if calendar
        else "trading_date DATE,symbol VARCHAR,underlying_symbol VARCHAR,roll_date DATE"
    )
    connection.execute(f"CREATE TEMP TABLE IF NOT EXISTS {incoming} ({definition})")
    connection.execute(f"DELETE FROM {incoming}")
    values = (
        [(row.date, row.trading) for row in records]
        if calendar
        else [
            (
                row.date,
                symbol,
                row.underlying_symbol,
                mapping_nodes_for([row], nodes or [])[0].date,
            )
            for row in records
        ]
    )
    marks = ",".join("?" for _ in values[0])
    connection.executemany(f"INSERT INTO {incoming} VALUES ({marks})", values)
    segment, absorbed, coverage = select_segment(
        connection, key, kind, table, column, incoming
    )
    first, last = records[0].date.toordinal(), records[-1].date.toordinal()
    connection.execute(
        "INSERT OR IGNORE INTO cache_segments VALUES (?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?,'date')",
        [segment, key, first, first, last, len(records), kind],
    )
    if absorbed:
        connection.execute(
            f"INSERT OR IGNORE INTO {table} (segment_id,{columns}) SELECT ?,{columns} FROM {table} WHERE segment_id IN (SELECT unnest(?))",
            [segment, absorbed],
        )
        if not calendar:
            connection.execute(
                "INSERT OR IGNORE INTO mapping_context SELECT ?,roll_date,underlying_symbol,old_symbol FROM mapping_context WHERE segment_id IN (SELECT unnest(?))",
                [segment, absorbed],
            )
            connection.execute(
                "DELETE FROM mapping_context WHERE segment_id IN (SELECT unnest(?))",
                [absorbed],
            )
        connection.execute(
            f"DELETE FROM {table} WHERE segment_id IN (SELECT unnest(?))", [absorbed]
        )
        connection.execute(
            "DELETE FROM cache_segments WHERE segment_id IN (SELECT unnest(?))",
            [absorbed],
        )
    update = (
        "trading=excluded.trading"
        if calendar
        else "symbol=excluded.symbol,underlying_symbol=excluded.underlying_symbol,roll_date=excluded.roll_date"
    )
    connection.execute(
        f"INSERT INTO {table} (segment_id,{columns}) SELECT ?,{columns} FROM {incoming} ON CONFLICT(segment_id,{column}) DO UPDATE SET {update}",
        [segment],
    )
    first_date, last_date, count = connection.execute(
        f"SELECT MIN({column}),MAX({column}),COUNT(*) FROM {table} WHERE segment_id=?",
        [segment],
    ).fetchone()
    connection.execute(
        "UPDATE cache_segments SET covered_from=?,first_time=?,last_time=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE segment_id=?",
        [
            min([first, *coverage]),
            first_date.toordinal(),
            last_date.toordinal(),
            count,
            segment,
        ],
    )
    return segment


def _save_facts(connection, key: str, kind: str, facts) -> None:
    if isinstance(facts, CalendarFacts):
        values = [
            key,
            kind,
            facts.digest,
            None,
            facts.holiday_last,
            facts.valid_from,
            facts.valid_to,
            facts.server_time,
            None,
            None,
            None,
        ]
    else:
        values = [
            key,
            kind,
            facts.digest,
            facts.calendar_digest,
            None,
            None,
            None,
            facts.server_time,
            facts.verified_date,
            facts.underlying_symbol,
            facts.reference_time,
        ]
    connection.execute(
        "INSERT OR REPLACE INTO metadata_source_facts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        values,
    )


def submit_calendar(connection, result: CalendarSourceResult) -> None:
    if not result.records:
        raise ValueError("calendar batch must not be empty")
    validate_calendar(result.records, result.records[0].date, result.records[-1].date)
    if (
        not result.facts.valid_from
        <= result.records[0].date
        <= result.records[-1].date
        <= result.facts.valid_to
    ):
        raise ValueError("calendar rows exceed source coverage")
    key = metadata_key("calendar")
    _merge_dates(connection, key, "calendar", result.records)
    _save_facts(connection, key, "calendar", result.facts)


def submit_mapping(connection, result: MappingSourceResult) -> None:
    facts = result.facts
    dates = [row.date for row in result.records]
    nodes = [*result.nodes, result.verification_node]
    if dates != sorted(set(dates)) or any(day > facts.verified_date for day in dates):
        raise ValueError("mapping rows exceed verified date or are unordered")
    if any(node.date > facts.verified_date for node in nodes):
        raise ValueError("mapping nodes exceed verified date")
    if not facts.underlying_symbol or any(
        not row.underlying_symbol for row in result.records
    ):
        raise ValueError("empty mapping")
    if result.verification_node.underlying_symbol != facts.underlying_symbol:
        raise ValueError("verification node differs from current underlying")
    mapping_nodes_for(result.records, result.nodes)
    key = metadata_key("main_mapping", result.symbol)
    batches = [(result.records, result.nodes)] if result.records else []
    verification = MappingDay(facts.verified_date, facts.underlying_symbol)
    if facts.verified_date not in dates:
        batches.append(([verification], [result.verification_node]))
    elif result.records[-1].underlying_symbol != facts.underlying_symbol:
        raise ValueError("current mapping is inconsistent")
    for records, contexts in batches:
        segment = _merge_dates(
            connection, key, "main_mapping", records, result.symbol, contexts
        )
        for node in contexts:
            connection.execute(
                "INSERT INTO mapping_context VALUES (?,?,?,?) ON CONFLICT(segment_id,roll_date,underlying_symbol) DO UPDATE SET old_symbol=excluded.old_symbol",
                [segment, node.date, node.underlying_symbol, node.old_symbol],
            )
    _save_facts(connection, key, "main_mapping", facts)
