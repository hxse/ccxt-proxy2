from typing import Optional, Union
from pathlib import Path
import shutil

import pandas as pd
import numpy as np


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
