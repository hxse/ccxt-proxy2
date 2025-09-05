from typing import Optional, Union, List
from pathlib import Path
import shutil

import pandas as pd
import numpy as np
from pandas import Timestamp


def clear_cache_directory(cache_dir: Union[str, Path]) -> None:
    """
    清空指定的缓存目录。

    该函数会删除目录内的所有文件和子目录。

    Args:
        cache_dir: 缓存目录的路径，可以是字符串或 Path 对象。
    """
    cache_path = Path(cache_dir)

    # 检查路径是否存在，如果不存在则无需清空
    if not cache_path.exists():
        print(f"Warning: Directory not found at '{cache_dir}'. Skipping deletion.")
        return

    # 遍历并删除所有内容
    for item in cache_path.iterdir():
        if item.is_dir():
            # 删除子目录及其所有内容
            shutil.rmtree(item, ignore_errors=True)
        else:
            # 删除文件
            item.unlink()


def assert_uniform_time_intervals(df: pd.DataFrame, time_column: str = "time") -> None:
    """
    验证 DataFrame 中 time 列的时间间隔是否相等，不改变原始 DataFrame。

    Args:
        df (pd.DataFrame): 包含时间序列数据的 DataFrame。
        time_column (str): 时间戳列的名称，默认为 'time'。

    Raises:
        AssertionError: 如果时间间隔不相等，则抛出异常。
    """
    # 如果数据不足两行，则直接通过
    if len(df) < 2:
        print("✅ 验证通过：数据行数不足，无需检查间隔。")
        return

    # 创建 DataFrame 的副本，以避免修改原始数据
    temp_df = df.copy()

    # 转换时间列为 datetime 类型
    temp_df[time_column] = pd.to_datetime(temp_df[time_column])

    # 计算相邻时间戳的差值
    time_diffs = temp_df[time_column].diff()

    # 移除第一个 NaN 值并检查所有差值的唯一数量
    unique_diffs = time_diffs.dropna().nunique()

    if unique_diffs > 1:
        # 如果不只一种差值，则断言失败
        diff_counts = time_diffs.dropna().value_counts()
        error_message = f"时间间隔不相等。发现多种间隔：\n{diff_counts}"
        raise AssertionError(error_message)
    else:
        # 如果所有间隔都相等
        interval = time_diffs.iloc[1] if len(time_diffs) > 1 else "无"
        print(f"✅ 验证通过：所有时间间隔都相等，为 {interval}。")


def calculate_future_timestamp(
    start_time: Union[str, Timestamp], period: str, k_lines: int
) -> Timestamp:
    """
    计算给定起始时间后，经过指定周期和K线数量后的时间戳。

    Args:
        start_time (Union[str, Timestamp]): 起始时间戳，可以是字符串或pandas Timestamp对象。
        period (str): K线周期，例如 '1m' (1分钟), '15m' (15分钟), '1H' (1小时)。
        k_lines (int): 经过的K线数量。

    Returns:
        Timestamp: 计算后的未来时间戳。

    Raises:
        ValueError: 如果period格式无效，会抛出异常。
    """
    # 确保起始时间是pandas Timestamp对象
    start_time = pd.to_datetime(start_time)

    # 将周期字符串转换为pandas的Timedelta
    time_delta = pd.to_timedelta(period)

    # 计算总共经过的时间
    total_time_passed = time_delta * k_lines

    # 计算最终的时间戳
    future_time = start_time + total_time_passed

    return future_time


def calculate_kline_count(
    start_time: Union[str, Timestamp], end_time: Union[str, Timestamp], period: str
) -> int:
    """
    根据起始时间、结束时间和周期计算K线数量。

    Args:
        start_time (Union[str, Timestamp]): 起始时间戳，可以是字符串或pandas Timestamp对象。
        end_time (Union[str, Timestamp]): 结束时间戳，可以是字符串或pandas Timestamp对象。
        period (str): K线周期，例如 '1m' (1分钟), '15m' (15分钟), '1H' (1小时)。

    Returns:
        int: 这段时间内K线的数量。

    Raises:
        ValueError: 如果period格式无效或结束时间早于起始时间，会抛出异常。
    """
    # 1. 确保时间戳是pandas Timestamp对象
    start_time_ts = pd.to_datetime(start_time)
    end_time_ts = pd.to_datetime(end_time)

    # 2. 检查结束时间是否早于起始时间
    if end_time_ts < start_time_ts:
        raise ValueError("结束时间不能早于起始时间。")

    # 3. 将周期字符串转换为pandas的Timedelta
    period_timedelta = pd.to_timedelta(period)

    # 4. 计算总时间差并除以周期得到K线数量
    total_duration = end_time_ts - start_time_ts

    # 将总时长转换为秒，然后除以周期的秒数，确保类型匹配
    # 这里加1是因为你需要计算"包含"起始和结束时间点的K线数量
    count = int(total_duration / period_timedelta) + 1

    return count


import pandas as pd
from typing import List, Union
from pandas import Timestamp


def validate_merged_data(
    cached_data: pd.DataFrame,
    start_times: List[Union[str, int, Timestamp]],
    period: str,
    count: int,
) -> None:
    """
    验证合并后的 DataFrame 的时间范围和K线数量是否正确。

    Args:
        cached_data (pd.DataFrame): 合并后的数据 DataFrame。
        start_times (List[Union[str, int, Timestamp]]): 原始数据的起始时间列表。
        period (str): K线周期，例如 '15m'。
        count (int): 每段原始数据的K线数量。

    Raises:
        AssertionError: 如果合并后的数据时间范围或K线数量不符合预期，则抛出异常。
    """
    if cached_data.empty:
        print("✅ 验证通过：合并数据为空，无需进一步验证。")
        return

    # 将所有起始时间转换为 Timestamp 对象
    start_times_ts = [pd.to_datetime(t, unit="ms") for t in start_times]

    # 计算每个数据块的结束时间
    end_times_ts = [
        calculate_future_timestamp(start, period, k_lines=count - 1)
        for start in start_times_ts
    ]

    # 确定理论上合并后的首尾时间戳 (Timestamp对象)
    first_timestamp = min(start_times_ts)
    last_timestamp = max(end_times_ts)

    # 计算理论上的总K线数量
    expected_count = calculate_kline_count(first_timestamp, last_timestamp, period)

    # 将 Pandas Timestamp 对象转换为毫秒时间戳，以便与 DataFrame 中的整数值进行比较
    # .timestamp() 方法返回秒，需要乘以1000得到毫秒，然后转换为整数
    first_timestamp_ms = int(first_timestamp.timestamp() * 1000)
    last_timestamp_ms = int(last_timestamp.timestamp() * 1000)

    print("\n--- 预期的数据信息 ---")
    print(f"预期首个时间戳 (ms): {first_timestamp_ms}")
    print(f"预期最后一个时间戳 (ms): {last_timestamp_ms}")
    print(f"预期总行数 (K线数量): {expected_count}")

    print("\n--- 实际的缓存数据信息 ---")
    print(f"实际首个时间戳 (ms): {cached_data.iloc[0]['time']}")
    print(f"实际最后一个时间戳 (ms): {cached_data.iloc[-1]['time']}")
    print(f"实际总行数 (K线数量): {len(cached_data)}")

    # 添加断言验证计算结果
    assert cached_data.iloc[0]["time"] == first_timestamp_ms, (
        "实际首个时间戳与预期不符。"
    )
    assert cached_data.iloc[-1]["time"] == last_timestamp_ms, (
        "实际最后一个时间戳与预期不符。"
    )
    assert len(cached_data) == expected_count, "实际总行数与预期不符。"

    print("\n✅ 所有计算和断言验证通过，合并后的数据信息正确。")
