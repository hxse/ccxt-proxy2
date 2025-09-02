import pandas as pd
from pathlib import Path

from .cache_utils import get_sorted_cache_files, get_file_info
from .cache_file_io import write_to_cache, read_cache_file
from .cache_data_processor import merge_with_deduplication


def handle_overlap_at_end(
    sorted_cache_files: list[Path],
    new_data: pd.DataFrame,
    cache_size: int,
    file_type: str,
) -> tuple[pd.DataFrame, list[Path]]:
    """
    处理新数据结束时间点与旧数据开始时间点重叠的情况。
    如果旧文件大小小于 cache_size，则合并旧数据到新数据，并标记旧文件待删除。
    """
    files_to_delete = []
    new_data_end = new_data.iloc[-1, 0]

    for f in sorted_cache_files:
        info = get_file_info(f.name)
        if not info:
            continue
        old_data_start = info["start_time"]

        # 新数据结束时间点 == 旧数据开始时间点 且 旧数据大小小于 cache_size -> 合并旧数据到新数据，删除旧文件
        if new_data_end == old_data_start:
            # 检查旧数据大小，只有小于 cache_size 才考虑合并
            old_file_info = get_file_info(f.name)
            if old_file_info and old_file_info["count"] < cache_size:
                print(
                    f"🔄 新数据结束时间点 == 旧数据开始时间点，且旧文件小于 {cache_size}，进行合并。"
                )
                old_data = read_cache_file(f, file_type)
                new_data = merge_with_deduplication(new_data, old_data)
                files_to_delete.append(f)
                # 更新新数据的结束时间为合并后的结束时间
                new_data_end = new_data.iloc[-1, 0]
    return new_data, files_to_delete


def handle_overlap_at_start(
    sorted_cache_files: list[Path],
    new_data: pd.DataFrame,
    cache_size: int,
    file_type: str,
) -> tuple[pd.DataFrame, list[Path]]:
    """
    处理新数据开始时间点与旧数据结束时间点重叠的情况。
    如果旧文件大小小于 cache_size，则合并旧数据到新数据，并标记旧文件待删除。
    """
    files_to_delete = []
    new_data_start = new_data.iloc[0, 0]

    for f in sorted_cache_files[::-1]:
        info = get_file_info(f.name)
        if not info:
            continue
        old_data_end = info["end_time"]

        # 新数据开始时间点 == 旧数据结束时间点 且 旧数据大小小于 cache_size -> 合并旧数据到新数据，删除旧文件
        if new_data_start == old_data_end:
            # 检查旧数据大小，只有小于 cache_size 才考虑合并
            old_file_info = get_file_info(f.name)
            if old_file_info and old_file_info["count"] < cache_size:
                print(
                    f"🔄 新数据开始时间点 == 旧数据结束时间点，且旧文件小于 {cache_size}，进行合并。"
                )
                old_data = read_cache_file(f, file_type)
                new_data = merge_with_deduplication(old_data, new_data)  # 注意合并方向
                files_to_delete.append(f)
                # 更新新数据的开始时间为合并后的开始时间
                new_data_start = new_data.iloc[0, 0]
    return new_data, files_to_delete


def adjust_write_range_and_delete_overlapped(
    sorted_cache_files: list[Path],
    new_data: pd.DataFrame,
    new_data_start: int,
    new_data_end: int,
) -> tuple[int, int, list[Path]]:
    """
    调整新数据写入范围并标记完全覆盖的旧文件为待删除。
    """
    start_time_to_write = new_data_start
    end_time_to_write = new_data_end
    files_to_delete = []

    for f in sorted_cache_files:
        info = get_file_info(f.name)
        if not info:
            continue
        old_data_start = info["start_time"]
        old_data_end = info["end_time"]

        # 1. 新数据完全在旧数据内部（完全被缓存） -> 无需写入，直接返回
        if new_data_start >= old_data_start and new_data_end <= old_data_end:
            print(
                f"✅ 新数据 ({new_data_start}-{new_data_end}) 已被缓存文件 ({old_data_start}-{old_data_end}) 完全覆盖，无需写入。"
            )
            return -1, -1, []  # 返回特殊值表示无需写入

        # 2. 新数据完全覆盖旧数据 -> 标记旧文件为删除
        elif new_data_start <= old_data_start and new_data_end >= old_data_end:
            print(
                f"🔄 新数据 ({new_data_start}-{new_data_end}) 完全覆盖旧缓存文件 ({old_data_start}-{old_data_end})，标记为删除。"
            )
            files_to_delete.append(f)

        # 3. 新数据在旧数据之前且有重叠 -> 调整写入的结束时间，使其不包含重叠部分
        elif new_data_end > old_data_start and new_data_start < old_data_start:
            end_time_to_write = min(end_time_to_write, old_data_start)
            print(
                f"⚠️ 新数据 ({new_data_start}-{new_data_end}) 与旧缓存 ({old_data_start}-{old_data_end}) 重叠，调整写入结束时间。"
            )

        # 4. 新数据在旧数据之后且有重叠 -> 调整写入的起始时间，使其不包含重叠部分
        elif new_data_start < old_data_end and new_data_end > old_data_end:
            start_time_to_write = max(start_time_to_write, old_data_end)
            print(
                f"⚠️ 新数据 ({new_data_start}-{new_data_end}) 与旧缓存 ({old_data_start}-{old_data_end}) 重叠，调整写入起始时间。"
            )
    return start_time_to_write, end_time_to_write, files_to_delete


def handle_cache_write(
    symbol: str,
    period: str,
    new_data: pd.DataFrame,
    cache_dir: Path,
    cache_size: int,
    file_type: str = "parquet",
) -> None:
    """
    处理新数据写入缓存的逻辑，包括处理重叠和覆盖情况。

    此函数采用“先处理后写入”的策略，通过记录重叠边界，最后一次性对新数据进行切片并写入。
    """
    if new_data.empty:
        return

    # 处理新数据结束时间点与旧数据开始时间点重叠的情况
    sorted_cache_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    new_data, files_to_delete_end = handle_overlap_at_end(
        sorted_cache_files, new_data, cache_size, file_type
    )
    for f in files_to_delete_end:
        if f.exists():
            print(f"🗑️ 删除旧缓存文件: {f.name}")
            f.unlink()

    # 处理新数据开始时间点与旧数据结束时间点重叠的情况
    sorted_cache_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    new_data, files_to_delete_start = handle_overlap_at_start(
        sorted_cache_files, new_data, cache_size, file_type
    )
    for f in files_to_delete_start:
        if f.exists():
            print(f"🗑️ 删除旧缓存文件: {f.name}")
            f.unlink()

    # 调整写入范围并标记完全覆盖的旧文件为待删除
    sorted_cache_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)
    new_data_start = new_data.iloc[0, 0]
    new_data_end = new_data.iloc[-1, 0]
    start_time_to_write, end_time_to_write, files_to_delete_overlap = (
        adjust_write_range_and_delete_overlapped(
            sorted_cache_files, new_data, new_data_start, new_data_end
        )
    )
    for f in files_to_delete_overlap:
        if f.exists():
            print(f"🗑️ 删除旧缓存文件: {f.name}")
            f.unlink()

    if start_time_to_write == -1 and end_time_to_write == -1:
        # 表示新数据已完全被缓存文件覆盖，无需写入
        return

    # 最后根据调整后的时间范围进行切片和写入
    data_to_write = new_data[
        (new_data["time"] >= start_time_to_write)
        & (new_data["time"] <= end_time_to_write)
    ]

    if not data_to_write.empty:
        write_to_cache(symbol, period, data_to_write, cache_dir, cache_size, file_type)
    else:
        print("❌ 经过处理，没有数据需要写入缓存。")
