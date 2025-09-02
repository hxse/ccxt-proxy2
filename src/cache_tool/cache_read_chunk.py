import pandas as pd
from pathlib import Path

from .cache_utils import get_sorted_cache_files, get_file_info
from .cache_file_io import read_cache_file
from .cache_data_processor import merge_with_deduplication


def get_next_continuous_cache_chunk(
    cache_dir: Path,
    symbol: str,
    period: str,
    start_time: int,
    target_count: int,
    file_type: str = "parquet",
) -> pd.DataFrame:
    """
    寻找并加载从指定时间开始的连续缓存数据块。
    """
    cached_data = pd.DataFrame()

    sorted_files = get_sorted_cache_files(cache_dir, symbol, period, file_type)

    start_file = None

    # 遍历一次所有文件，根据优先级找到最佳匹配
    for f in sorted_files:
        info = get_file_info(f.name)
        if not info:
            continue

        if info["start_time"] <= start_time <= info["end_time"]:
            start_file = f
            print("✅ 找到匹配文件。")
            break

    if start_file is None:
        return cached_data

    # 从匹配的文件开始加载数据
    first_chunk = read_cache_file(start_file, file_type)

    # 精确切片：找到 start_time 对应的行并切片
    try:
        start_index = first_chunk.index[first_chunk["time"] == start_time][0]
        cached_data = first_chunk.iloc[start_index:]
    except IndexError:
        # 如果找不到精确匹配的索引，则返回空 DataFrame
        return pd.DataFrame()

    # 将当前时间更新为已加载数据的最后一个时间点，用于后续的连续性检查
    current_time = cached_data.iloc[-1, 0]

    # 继续加载后续连续的缓存文件
    start_file_index = sorted_files.index(start_file)
    for i in range(start_file_index + 1, len(sorted_files)):
        filepath = sorted_files[i]
        info = get_file_info(filepath.name)

        # 检查是否连续：下一个文件的起始时间是否等于当前已加载数据的结束时间
        if info["start_time"] == current_time:
            chunk = read_cache_file(filepath, file_type)

            cached_data = merge_with_deduplication(cached_data, chunk)

            # 更新当前时间为新合并数据的最后一个时间点
            current_time = cached_data.iloc[-1, 0]

            if len(cached_data) >= target_count:
                # 如果已加载的数据量达到目标数量，进行切片并停止加载
                cached_data = cached_data.iloc[:target_count]
                break
        else:
            # 文件不连续，停止查找
            break

    return cached_data
