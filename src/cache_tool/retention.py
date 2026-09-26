"""逐身份短事务的逻辑清理；不请求网络、不执行物理 compact。"""

import json
from datetime import date, datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import duckdb

from src.cache_tool.maintenance_models import MaintenanceFailure, RetentionPolicy


def _identities(connection):
    return connection.execute(
        "SELECT DISTINCT series_key,data_kind FROM cache_segments ORDER BY data_kind,series_key"
    ).fetchall()


def _ids(connection, key, kind):
    return [
        row[0]
        for row in connection.execute(
            "SELECT segment_id FROM cache_segments WHERE series_key=? AND data_kind=? ORDER BY segment_id",
            [key, kind],
        ).fetchall()
    ]


def _ordinary(cache, connection, key, limit):
    before = cache._count_series(connection, key)
    if before > limit:
        cache._evict_series(connection, key, before - limit)
        cache._refresh_metadata(connection)
    return before - cache._count_series(connection, key)


def _refresh_date(connection, segment, table, column, kind):
    bounds = connection.execute(
        f"SELECT MIN({column}),MAX({column}),COUNT(*) FROM {table} WHERE segment_id=?",
        [segment],
    ).fetchone()
    if not bounds[2]:
        if kind == "main_mapping":
            connection.execute(
                "DELETE FROM mapping_context WHERE segment_id=?", [segment]
            )
        connection.execute("DELETE FROM cache_segments WHERE segment_id=?", [segment])
        return
    first, last, count = bounds[0].toordinal(), bounds[1].toordinal(), bounds[2]
    connection.execute(
        """
        UPDATE cache_segments SET covered_from=CASE WHEN first_time<? THEN ? ELSE covered_from END,
            first_time=?,last_time=?,row_count=?,updated_at=CURRENT_TIMESTAMP WHERE segment_id=?
    """,
        [first, first, first, last, count, segment],
    )
    if kind == "main_mapping":
        connection.execute(
            """DELETE FROM mapping_context c WHERE c.segment_id=? AND NOT EXISTS (
                SELECT 1 FROM mapping_rows r WHERE r.segment_id=c.segment_id
                AND ((r.roll_date=c.roll_date AND r.underlying_symbol=c.underlying_symbol)
                     OR (r.roll_date IS NULL AND c.roll_date<=r.trading_date))
            )""",
            [segment],
        )


def _dates(connection, key, kind, cutoff):
    table, column = (
        ("calendar_rows", "date")
        if kind == "calendar"
        else ("mapping_rows", "trading_date")
    )
    total = 0
    for segment in _ids(connection, key, kind):
        total += connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE segment_id=? AND {column}<?",
            [segment, cutoff],
        ).fetchone()[0]
        connection.execute(
            f"DELETE FROM {table} WHERE segment_id=? AND {column}<?", [segment, cutoff]
        )
        _refresh_date(connection, segment, table, column, kind)
    return total


def _window(connection, key, cutoff):
    delta = datetime.combine(
        cutoff, datetime.min.time(), ZoneInfo("Asia/Shanghai")
    ) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    cutoff_ns = (delta.days * 86400 + delta.seconds) * 1_000_000_000
    total = 0
    for segment in _ids(connection, key, "transition"):
        count, last = connection.execute(
            "SELECT COUNT(*),MAX(datetime) FROM transition_rows WHERE segment_id=?",
            [segment],
        ).fetchone()
        if count and last >= cutoff_ns:
            continue
        total += count
        connection.execute("DELETE FROM transition_rows WHERE segment_id=?", [segment])
        connection.execute(
            "DELETE FROM transition_windows WHERE segment_id=?", [segment]
        )
        connection.execute("DELETE FROM cache_segments WHERE segment_id=?", [segment])
    return total


def prune(cache, policy: RetentionPolicy, cutoff: date | None):
    auxiliary = policy.keep_years is not None
    report: dict[str, Any] = {
        "status": "completed",
        "rules": policy.rules(),
        "ohlcv": {"series_processed": 0, "deleted_rows": 0},
        "auxiliary": {
            "status": "completed"
            if auxiliary and cutoff
            else "skipped"
            if auxiliary
            else "not_requested",
            "cutoff_date": cutoff.isoformat() if auxiliary and cutoff else None,
            "deleted_rows": 0,
        },
        "errors": [],
    }
    if auxiliary and cutoff is None:
        report["errors"].append(
            {"scope": "auxiliary", "code": "TRUSTED_TIME_UNAVAILABLE"}
        )
    try:
        with cache._read_transaction() as connection:
            identities = _identities(connection)
        for key, kind in identities:
            identity = json.loads(key)
            category = "tq" if identity["provider"] == "tq" else "ccxt"
            if category not in policy.providers:
                continue
            if kind != "ohlcv" and (
                not auxiliary or cutoff is None or category != "tq"
            ):
                continue
            scope = "ohlcv" if kind == "ohlcv" else "auxiliary"
            try:
                # close 与每个事务共享门禁；关闭后不启动下一身份的 SQL。
                with cache._transaction() as connection:
                    if kind == "ohlcv":
                        limit = {"live": policy.live, "sandbox": policy.sandbox}[
                            identity["mode"]
                        ]
                        deleted = _ordinary(cache, connection, key, limit)
                    elif kind in {"calendar", "main_mapping"}:
                        deleted = _dates(connection, key, kind, cutoff)
                    elif kind == "transition":
                        deleted = _window(connection, key, cutoff)
                    else:
                        raise ValueError("unknown cache data kind")
                report[scope]["deleted_rows"] += deleted
                if kind == "ohlcv":
                    report["ohlcv"]["series_processed"] += 1
            except (
                duckdb.CatalogException,
                duckdb.IOException,
                duckdb.ConnectionException,
                duckdb.FatalException,
            ):
                raise
            except RuntimeError:
                if cache._closed or cache._closing:
                    report["errors"].append(
                        {"scope": scope, "code": "CACHE_MAINTENANCE_STOPPED"}
                    )
                    break
                report["errors"].append(
                    {
                        "scope": scope,
                        "identity": identity,
                        "code": "CACHE_SERIES_PRUNE_FAILED",
                    }
                )
            except Exception:
                report["errors"].append(
                    {
                        "scope": scope,
                        "identity": identity,
                        "code": "CACHE_SERIES_PRUNE_FAILED",
                    }
                )
    except Exception as exc:
        if cache._closing or cache._closed:
            report["status"] = "partial"
            report["errors"].append(
                {"scope": "database", "code": "CACHE_MAINTENANCE_STOPPED"}
            )
            return report
        report["status"] = "partial"
        report["errors"].append(
            {"scope": "database", "code": "CACHE_MAINTENANCE_FAILED"}
        )
        raise MaintenanceFailure(report) from exc
    if report["errors"]:
        report["status"] = "partial"
    return report
