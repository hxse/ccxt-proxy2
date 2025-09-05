import pandas as pd
import pytest
from pathlib import Path
from src.cache_tool.cache_entry import get_ohlcv_with_cache, mock_fetch_ohlcv
from src.cache_tool.cache_utils import parse_timestamp_string
from src.cache_tool.cache_file_io import read_cache_file
from Test.utils import clear_cache_directory, assert_uniform_time_intervals
from src.cache_tool.cache_data_processor import merge_with_deduplication

from dataclasses import dataclass
from typing import Optional


@pytest.fixture(scope="module")
def cache_setup():
    """
    为测试提供一个临时缓存目录，并在测试结束后清理。
    """
    # 创建一个临时目录用于缓存
    temp_cache_dir = Path("test_database")
    temp_cache_dir.mkdir(parents=True, exist_ok=True)
    yield temp_cache_dir
    # 清理临时目录下的所有文件和子目录

    clear_cache_directory(temp_cache_dir)


@dataclass
class CacheTestParams:
    """
    用于 get_ohlcv_with_cache 测试用例的参数集合。
    """

    symbol: str = "BTC/USDT"
    period: str = "15m"
    start_time: int = parse_timestamp_string("20230101T060000Z")
    count: int = 15
    cache_dir: Optional[Path] = None
    cache_size: int = 5
    page_size: int = 10
    enable_cache: bool = True
    file_type: str = "csv"


def test_get_ohlcv_with_cache(cache_setup):
    print("\ntest_get_ohlcv_with_cache_consolidate")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 第一次调用：正常请求并写入缓存
    print("\n--- 第一次调用: 请求并写入缓存 ---")
    df_write = get_ohlcv_with_cache(
        **vars(tp), fetch_callback=mock_fetch_ohlcv, enable_consolidate=False
    )

    assert not df_write.empty, "第一次调用返回的DataFrame不应为空"
    assert len(df_write) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    # 验证缓存文件是否已创建
    cache_files = list(tp.cache_dir.rglob(f"*.{tp.file_type}"))
    assert len(cache_files) > 0, "缓存目录中应存在缓存文件"

    cached_data = pd.DataFrame()
    for i in cache_files:
        chunk = read_cache_file(i, tp.file_type)
        cached_data = merge_with_deduplication(cached_data, chunk)

    assert len(cached_data) == tp.count, (
        f"cached_data行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    assert_uniform_time_intervals(df_write, "time")
    assert_uniform_time_intervals(cached_data, "time")

    clear_cache_directory(tp.cache_dir)


def test_get_ohlcv_with_cache_consolidate(cache_setup):
    print("\ntest_get_ohlcv_with_cache_consolidate")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 第一次调用：正常请求并写入缓存
    print("\n--- 第一次调用: 请求并写入缓存 ---")
    df_write = get_ohlcv_with_cache(
        **vars(tp), fetch_callback=mock_fetch_ohlcv, enable_consolidate=True
    )

    assert not df_write.empty, "第一次调用返回的DataFrame不应为空"
    assert len(df_write) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    # 验证缓存文件是否已创建
    cache_files = list(tp.cache_dir.rglob(f"*.{tp.file_type}"))
    assert len(cache_files) > 0, "缓存目录中应存在缓存文件"

    cached_data = pd.DataFrame()
    for i in cache_files:
        chunk = read_cache_file(i, tp.file_type)
        cached_data = merge_with_deduplication(cached_data, chunk)

    assert len(cached_data) == tp.count, (
        f"cached_data行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    assert_uniform_time_intervals(df_write, "time")
    assert_uniform_time_intervals(cached_data, "time")

    clear_cache_directory(tp.cache_dir)


def test_get_ohlcv_with_cache_consolidate2(cache_setup):
    print("\nest_get_ohlcv_with_cache_consolidate2")
    tp = CacheTestParams(cache_dir=cache_setup)

    enable_consolidate = False

    # 第一次调用：正常请求并写入缓存
    print("\n--- 第一次调用: 请求并写入缓存 ---")
    df_write = get_ohlcv_with_cache(
        **vars(tp),
        fetch_callback=mock_fetch_ohlcv,
        enable_consolidate=enable_consolidate,
    )
    tp.start_time = parse_timestamp_string("20230101T051500Z")
    df_write2 = get_ohlcv_with_cache(
        **vars(tp),
        fetch_callback=mock_fetch_ohlcv,
        enable_consolidate=enable_consolidate,
    )
    import pdb

    pdb.set_trace()
    tp.start_time = parse_timestamp_string("20230101T083000Z")
    df_write3 = get_ohlcv_with_cache(
        **vars(tp),
        fetch_callback=mock_fetch_ohlcv,
        enable_consolidate=enable_consolidate,
    )
    import pdb

    pdb.set_trace()

    assert len(df_write) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write)}"
    )
    print(
        "df_write", len(df_write), df_write.iloc[0]["date"], df_write.iloc[-1]["date"]
    )
    assert len(df_write2) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write2)}"
    )
    print(
        "df_write2",
        len(df_write2),
        df_write2.iloc[0]["date"],
        df_write2.iloc[-1]["date"],
    )
    assert len(df_write3) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write3)}"
    )
    print(
        "df_write3",
        len(df_write3),
        df_write3.iloc[0]["date"],
        df_write3.iloc[-1]["date"],
    )

    print(len(df_write))
    df_write_merge = merge_with_deduplication(df_write, df_write2)
    print(
        len(df_write_merge),
    )
    df_write_merge = merge_with_deduplication(df_write_merge, df_write3)
    print(
        len(df_write_merge),
    )

    # 验证缓存文件是否已创建
    cache_files = list(tp.cache_dir.rglob(f"*.{tp.file_type}"))
    assert len(cache_files) > 0, "缓存目录中应存在缓存文件"

    cached_data = pd.DataFrame()
    for i in cache_files:
        chunk = read_cache_file(i, tp.file_type)
        cached_data = merge_with_deduplication(cached_data, chunk)

    assert len(cached_data) == len(df_write_merge), (
        f"cached_data行数应为 {len(df_write_merge)}，但实际为 {len(df_write)}"
    )

    assert_uniform_time_intervals(df_write, "time")
    assert_uniform_time_intervals(cached_data, "time")

    clear_cache_directory(tp.cache_dir)
