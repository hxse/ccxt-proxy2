import warnings

import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from typing import cast
from .models import LogEntry

EntrySortKey = tuple[int, int, str, str, int]
NormalizedEntry = tuple[int, int, str, str, int | None]


def _entry_sort_key(entry: LogEntry) -> EntrySortKey:
    """全序排序键，确保日志顺序稳定可复现。"""
    return (
        entry.data_start,
        entry.data_end,
        entry.fetch_time.isoformat(),
        entry.source,
        -1 if entry.count is None else entry.count,
    )


def _normalized_entries(entries: list[LogEntry]) -> list[NormalizedEntry]:
    """将日志条目转换为可比较的规范化序列。"""
    return [
        (
            e.data_start,
            e.data_end,
            e.fetch_time.isoformat(),
            e.source,
            e.count,
        )
        for e in entries
    ]


def get_log_path(data_dir: Path) -> Path:
    return data_dir / "fetch_log.jsonl"


def append_log(
    data_dir: Path,
    data_start: int,
    data_end: int,
    count: int,
    source: str = "api",
) -> None:
    """追加一条获取日志"""
    log_path = get_log_path(data_dir)

    entry = LogEntry(
        fetch_time=datetime.now(timezone.utc),
        data_start=data_start,
        data_end=data_end,
        count=count,
        source=source,
    )

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(entry.model_dump_json() + "\n")


def read_log(data_dir: Path) -> list[LogEntry]:
    """
    读取日志为 LogEntry 列表。

    如果日志文件损坏（包含无法解析的行），会打印警告并自动重建日志。
    """
    log_path = get_log_path(data_dir)

    if not log_path.exists():
        return []

    entries: list[LogEntry] = []
    corrupted = False

    with open(log_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            if line.strip():
                try:
                    entries.append(LogEntry.model_validate_json(line))
                except Exception as e:
                    warnings.warn(f"日志损坏 {log_path}:{line_num}，将触发重建: {e}")
                    corrupted = True
                    break  # 发现损坏后停止读取

    if corrupted:
        # 清空已读取的条目，重建日志
        rebuild_log_from_data(data_dir)
        # 递归重新读取重建后的日志
        return read_log(data_dir)

    # 规范化排序（全序）
    entries.sort(key=_entry_sort_key)
    return entries


def can_merge(entry_a: LogEntry, entry_b: LogEntry) -> bool:
    """
    判断两条日志是否可以合并

    合并条件（满足任一）：
    1. 首尾衔接: entry_a.data_end == entry_b.data_start
    2. 重叠或包含: 两条日志有交集
    """
    # 首尾衔接
    if entry_a.data_end == entry_b.data_start:
        return True
    if entry_b.data_end == entry_a.data_start:
        return True

    # 重叠或包含
    if (
        entry_a.data_start <= entry_b.data_end
        and entry_b.data_start <= entry_a.data_end
    ):
        return True

    return False


def compact_log(data_dir: Path) -> bool:
    """
    合并可合并的日志条目，减少日志行数

    合并条件：首尾衔接 或 重叠/包含

    Returns:
        是否发生了实际变化（并重写文件）
    """
    entries = read_log(data_dir)

    if len(entries) < 2:
        return False

    original = _normalized_entries(entries)

    compacted: list[LogEntry] = [entries[0]]

    for entry in entries[1:]:
        last = compacted[-1]

        if can_merge(last, entry):
            # 合并：取更大的范围
            merged = LogEntry(
                fetch_time=last.fetch_time,
                data_start=min(last.data_start, entry.data_start),
                data_end=max(last.data_end, entry.data_end),
                count=None,  # 合并后条数不再准确，设为 None
                source="compacted",
            )
            compacted[-1] = merged
        else:
            compacted.append(entry)

    # 规范化排序 + 比较是否有变化
    compacted.sort(key=_entry_sort_key)
    if _normalized_entries(compacted) == original:
        return False

    # 重写日志文件
    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        for entry in compacted:
            f.write(entry.model_dump_json() + "\n")
    return True


def rebuild_log_from_data(data_dir: Path) -> None:
    """
    从数据文件重建日志（用于日志丢失时恢复）

    注意：由于无法从数据中准确检测断裂（时间间隔不恒定），
    此函数将所有数据视为一个连续段。如果数据实际存在断裂，
    需要在日后查询时自然发现并补充。
    """
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        return

    rebuilt_entries: list[LogEntry] = []

    # 先收集每个分区文件的真实时间范围，再按首尾衔接规则合并
    for parquet_file in parquet_files:
        df = pl.read_parquet(parquet_file)
        if df.is_empty():
            continue

        rebuilt_entries.append(
            LogEntry(
                fetch_time=datetime.now(timezone.utc),
                data_start=cast(int, df["time"].min()),
                data_end=cast(int, df["time"].max()),
                count=len(df),
                source="rebuilt",
            )
        )

    if not rebuilt_entries:
        return

    rebuilt_entries.sort(key=_entry_sort_key)

    merged: list[LogEntry] = [rebuilt_entries[0]]
    for entry in rebuilt_entries[1:]:
        last = merged[-1]
        if can_merge(last, entry):
            merged[-1] = LogEntry(
                fetch_time=last.fetch_time,
                data_start=min(last.data_start, entry.data_start),
                data_end=max(last.data_end, entry.data_end),
                count=None,
                source="rebuilt",
            )
        else:
            merged.append(entry)

    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        for entry in merged:
            f.write(entry.model_dump_json() + "\n")
