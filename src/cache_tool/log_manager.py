import warnings

import polars as pl
from pathlib import Path
from datetime import datetime, timezone
from .models import LogEntry


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

    # 按 data_start 排序
    entries.sort(key=lambda e: e.data_start)
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


def compact_log(data_dir: Path) -> None:
    """
    合并可合并的日志条目，减少日志行数

    合并条件：首尾衔接 或 重叠/包含
    """
    entries = read_log(data_dir)

    if len(entries) < 2:
        return

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

    # 重写日志文件
    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        for entry in compacted:
            f.write(entry.model_dump_json() + "\n")


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

    dfs = [pl.read_parquet(f) for f in parquet_files]
    df = pl.concat(dfs).sort("time")

    if df.is_empty():
        return

    # 将整个数据视为一个连续段（保守策略）
    entry = LogEntry(
        fetch_time=datetime.now(timezone.utc),
        data_start=int(df["time"].min()),  # type: ignore
        data_end=int(df["time"].max()),  # type: ignore
        count=len(df),
        source="rebuilt",
    )

    log_path = get_log_path(data_dir)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(entry.model_dump_json() + "\n")
