import pandas as pd
from pathlib import Path

from .cache_utils import get_sorted_cache_files
from .cache_file_io import write_to_cache
from .cache_write_overlap_handler import (
    handle_overlap_at_end,
    handle_overlap_at_start,
    adjust_write_range_and_delete_overlapped,
)


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
