from pathlib import Path
from .models import Gap, DataRange
from .log_manager import read_log, can_merge


def check_continuity(data_dir: Path) -> list[Gap]:
    """
    检查数据连续性，返回断裂点列表。

    连续性规则：首尾衔接 或 重叠/包含
    """
    entries = read_log(data_dir)

    if len(entries) < 2:
        return []

    gaps: list[Gap] = []

    for i in range(1, len(entries)):
        prev = entries[i - 1]
        curr = entries[i]

        if not can_merge(prev, curr):
            gaps.append(
                Gap(
                    gap_after=prev.data_end,
                    gap_before=curr.data_start,
                )
            )

    return gaps


def get_data_range(data_dir: Path) -> DataRange | None:
    """获取已有数据的时间范围"""
    entries = read_log(data_dir)

    if not entries:
        return None

    return DataRange(
        start=min(e.data_start for e in entries),
        end=max(e.data_end for e in entries),
    )


def find_missing_ranges(
    data_dir: Path,
    target_start: int,
    target_end: int,
) -> list[DataRange]:
    """
    找出目标时间范围内缺失的数据段。
    用于增量下载。
    """
    data_range = get_data_range(data_dir)
    gaps = check_continuity(data_dir)

    missing: list[DataRange] = []

    # 1. 完全没有数据
    if data_range is None:
        missing.append(DataRange(start=target_start, end=target_end))
        return missing

    # 2. 目标范围之前的缺失
    if target_start < data_range.start:
        missing.append(DataRange(start=target_start, end=data_range.start))

    # 3. 中间的断裂
    for gap in gaps:
        if gap.gap_after >= target_start and gap.gap_before <= target_end:
            missing.append(DataRange(start=gap.gap_after, end=gap.gap_before))

    # 4. 目标范围之后的缺失
    if target_end > data_range.end:
        missing.append(DataRange(start=data_range.end, end=target_end))

    return missing
