import pandas as pd
import pytest
from src.cache_tool.cache_entry import get_ohlcv_with_cache, mock_fetch_ohlcv
from src.cache_tool.cache_utils import get_sorted_cache_files
from src.cache_tool.cache_file_io import read_cache_file
from Test.utils import assert_uniform_time_intervals, clear_cache_directory
from src.cache_tool.cache_data_processor import merge_with_deduplication
from Test.conftest import CacheTestParams


@pytest.mark.parametrize("enable_consolidate", [True, False])
def test_get_ohlcv_with_cache_consolidate(cache_setup, enable_consolidate):
    print(
        f"\ntest_get_ohlcv_with_cache_consolidate with enable_consolidate={enable_consolidate}"
    )
    tp = CacheTestParams(cache_dir=cache_setup)

    # 第一次调用：正常请求并写入缓存
    print("\n--- 第一次调用: 请求并写入缓存 ---")
    df_write = get_ohlcv_with_cache(
        **vars(tp),
        fetch_callback=mock_fetch_ohlcv,
        enable_consolidate=enable_consolidate,
    )

    assert len(df_write) == tp.count, (
        f"df_write行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    # 验证缓存文件是否已创建
    cache_files = get_sorted_cache_files(
        tp.cache_dir, tp.symbol, tp.period, tp.file_type
    )
    cached_data = pd.DataFrame()
    for i in cache_files:
        chunk = read_cache_file(i, tp.file_type)
        cached_data = merge_with_deduplication(cached_data, chunk)

    assert len(cached_data) == tp.count, (
        f"cached_data行数应为 {tp.count}，但实际为 {len(cached_data)}"
    )

    assert_uniform_time_intervals(df_write, "time")
    assert_uniform_time_intervals(cached_data, "time")

    clear_cache_directory(tp.cache_dir)
