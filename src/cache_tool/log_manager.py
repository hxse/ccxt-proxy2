from pathlib import Path
from datetime import datetime, timezone
from .models import LogEntry

EntrySortKey = tuple[int, int, str, str, int]
NormalizedEntry = tuple[int, int, str, str, int | None]


class CorruptedProofLogError(Exception):
    """proof log 损坏，不能再用于连续性判断。"""

    def __init__(self, log_path: Path, line_num: int, reason: str):
        self.log_path = log_path
        self.line_num = line_num
        self.reason = reason
        super().__init__(f"corrupted proof log {log_path}:{line_num}: {reason}")


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

    # 不能继续向损坏的 proof log 追加内容。
    if log_path.exists():
        read_log(data_dir)

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

    如果日志文件损坏（包含无法解析的行），抛出 CorruptedProofLogError。
    """
    log_path = get_log_path(data_dir)

    if not log_path.exists():
        return []

    entries: list[LogEntry] = []

    with open(log_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            if line.strip():
                try:
                    entries.append(LogEntry.model_validate_json(line))
                except Exception as e:
                    raise CorruptedProofLogError(log_path, line_num, str(e)) from e

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
