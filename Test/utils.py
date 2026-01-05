import polars as pl
from typing import Union, List
from pathlib import Path
import shutil
from datetime import datetime, timezone
import re
from src.cache_tool.cache_utils import (
    convert_ms_timestamp_to_utc_datetime,
    parse_timestamp_string,
)


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


def assert_uniform_time_intervals(df: pl.DataFrame, time_column: str = "time") -> None:
    """
    验证 DataFrame 中 time 列的时间间隔是否相等，不改变原始 DataFrame。

    Args:
        df (pl.DataFrame): 包含时间序列数据的 DataFrame。
        time_column (str): 时间戳列的名称，默认为 'time'。

    Raises:
        AssertionError: 如果时间间隔不相等，则抛出异常。
    """
    # 如果数据不足两行，则直接通过
    if len(df) < 2:
        print("✅ 验证通过：数据行数不足，无需检查间隔。")
        return

    # 转换时间列为 Datetime 类型并计算相邻时间戳的差值time_diffs_series = df[time_column].cast(pl.Datetime(time_unit="ms")).diff()
    time_diffs_series = df[time_column].cast(pl.Datetime(time_unit="ms")).diff()

    # 移除第一个 NaN 值并检查所有差值的唯一数量
    unique_diffs = time_diffs_series.drop_nulls().n_unique()

    if unique_diffs > 1:
        # 如果不只一种差值，则断言失败
        diff_counts = time_diffs_series.drop_nulls().value_counts()
        error_message = f"时间间隔不相等。发现多种间隔：\n{diff_counts}"
        raise AssertionError(error_message)
    else:
        # 如果所有间隔都相等
        interval = time_diffs_series.item(1) if len(time_diffs_series) > 1 else "无"
        print(f"✅ 验证通过：所有时间间隔都相等，为 {interval}。")


def _to_duration(period: str) -> pl.Expr:
    """将周期字符串转换为 Polars Duration 对象。"""
    match = re.match(r"(\d+)(\w+)", period)
    if not match:
        raise ValueError("无效的 period 格式")

    num, unit = match.groups()
    num = int(num)

    unit_map = {
        "s": "seconds",
        "m": "minutes",
        "h": "hours",
        "d": "days",
        "w": "weeks",
    }

    unit_plural = unit_map.get(unit)
    if not unit_plural:
        raise ValueError(f"无效的 period 单位: {unit}")

    return pl.duration(**{unit_plural: num})  # type: ignore


def _to_datetime(_time: Union[str, int, datetime]) -> datetime:
    """
    将多种时间类型转换为原生的 UTC datetime 对象，无需额外库。
    """
    if isinstance(_time, int):
        return convert_ms_timestamp_to_utc_datetime(_time)
    if isinstance(_time, str):
        return convert_ms_timestamp_to_utc_datetime(parse_timestamp_string(_time))

    if isinstance(_time, datetime):
        # 对于传入的原生 datetime 对象
        if _time.tzinfo is None:
            # 如果是天真的 datetime，假设它代表 UTC，并设置时区
            return _time.replace(tzinfo=timezone.utc)
        else:
            # 如果有其他时区，转换为 UTC
            return _time.astimezone(timezone.utc)
    return _time


def calculate_future_timestamps(
    start_times_series: pl.Series, period: str, k_lines: int
) -> pl.Series:
    """
    计算给定 Polars Series 中每个起始时间对应的未来时间戳。

    Args:
        start_times_series (pl.Series): 包含起始时间的 Polars Series。
        period (str): K线周期字符串，例如 "15m"。
        k_lines (int): 要计算的K线数量。

    Returns:
        pl.Series: 包含所有计算后未来时间戳的 Polars Series。
    """
    # 获取 Duration 表达式
    time_delta = _to_duration(period)

    # 计算偏移量表达式
    offset_expr = time_delta * (k_lines - 1)

    # 表达式求值
    future_time_series = pl.select(
        (pl.Series("start", start_times_series) + offset_expr)
    ).to_series()

    return future_time_series


def calculate_kline_count(
    start_time: Union[str, datetime],
    end_time: Union[str, datetime],
    period: str,
) -> int:
    """
    根据起始时间、结束时间和周期计算K线数量。
    """

    # 1. 确保时间戳是 Datetime 对象
    start_time_ts = _to_datetime(start_time)
    end_time_ts = _to_datetime(end_time)

    # 2. 检查结束时间是否早于起始时间
    if end_time_ts < start_time_ts:
        raise ValueError("结束时间不能早于起始时间。")

    # 3. 将周期字符串转换为 Polars 的 Duration
    period_timedelta = _to_duration(period)

    # 4. 计算总时间差
    total_duration = end_time_ts - start_time_ts

    # 5. 表达式求值
    count_expr = total_duration / period_timedelta
    count = int(pl.select(count_expr).item()) + 1
    return count


def validate_merged_data(
    cached_data: pl.DataFrame,
    start_times: List[Union[str, int, datetime]],
    period: str,
    count: int,
) -> None:
    """
    验证合并后的 DataFrame 的时间范围和K线数量是否正确。
    """
    if cached_data.is_empty():
        print("✅ 验证通过：合并数据为空，无需进一步验证。")
        return

    start_times_series = pl.Series([_to_datetime(t) for t in start_times])

    future_time_series = calculate_future_timestamps(start_times_series, period, count)

    first_timestamp = start_times_series.min()
    last_timestamp = future_time_series.max()

    if not isinstance(first_timestamp, datetime) or not isinstance(
        last_timestamp, datetime
    ):
        raise TypeError("Timestamp is not a datetime object")

    # 计算理论上的总K线数量
    expected_count = calculate_kline_count(first_timestamp, last_timestamp, period)

    # 将 Datetime 对象转换为毫秒时间戳
    first_timestamp_ms = int(first_timestamp.timestamp() * 1000)
    last_timestamp_ms = int(last_timestamp.timestamp() * 1000)

    print("\n--- 预期的数据信息 ---")
    print(f"预期首个时间戳 (ms): {first_timestamp_ms}")
    print(f"预期最后一个时间戳 (ms): {last_timestamp_ms}")
    print(f"预期总行数 (K线数量): {expected_count}")

    # 获取实际的首尾时间戳和总行数
    actual_first_ts = cached_data.head(1)["time"].item()
    actual_last_ts = cached_data.tail(1)["time"].item()
    actual_count = len(cached_data)

    print("\n--- 实际的缓存数据信息 ---")
    print(f"实际首个时间戳 (ms): {actual_first_ts}")
    print(f"实际最后一个时间戳 (ms): {actual_last_ts}")
    print(f"实际总行数 (K线数量): {actual_count}")

    # 添加断言验证计算结果
    assert actual_first_ts == first_timestamp_ms, "实际首个时间戳与预期不符。"
    assert actual_last_ts == last_timestamp_ms, "实际最后一个时间戳与预期不符。"
    assert actual_count == expected_count, "实际总行数与预期不符。"

    print("\n✅ 所有计算和断言验证通过，合并后的数据信息正确。")
