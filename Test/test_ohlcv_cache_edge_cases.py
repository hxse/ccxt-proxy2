import pandas as pd
import pytest
from src.cache_tool.cache_entry import get_ohlcv_with_cache, mock_fetch_ohlcv
from src.cache_tool.cache_utils import get_sorted_cache_files
from src.cache_tool.cache_file_io import read_cache_file
from Test.utils import clear_cache_directory, assert_uniform_time_intervals
from src.cache_tool.cache_data_processor import merge_with_deduplication
from Test.conftest import CacheTestParams


@pytest.mark.parametrize("count", [0, 1, 5])
def test_get_ohlcv_with_cache_edge_cases(cache_setup, count):
    print(f"\ntest_get_ohlcv_with_cache_edge_cases with count={count}")
    tp = CacheTestParams(cache_dir=cache_setup, count=count)

    # 清理缓存目录以确保测试独立性
    clear_cache_directory(tp.cache_dir)

    df_result = get_ohlcv_with_cache(
        **vars(tp), fetch_callback=mock_fetch_ohlcv, enable_consolidate=True
    )

    if count == 0:
        assert df_result.empty, (
            f"当count为0时，结果DataFrame应为空，但实际有 {len(df_result)} 行"
        )
        # 验证没有缓存文件被创建
        cache_files = get_sorted_cache_files(
            tp.cache_dir, tp.symbol, tp.period, tp.file_type
        )
        assert len(cache_files) == 0, "当count为0时，不应创建缓存文件"
    else:
        assert len(df_result) == count, (
            f"df_result行数应为 {count}，但实际为 {len(df_result)}"
        )
        # 验证缓存文件是否已创建并包含正确数量的数据
        cache_files = get_sorted_cache_files(
            tp.cache_dir, tp.symbol, tp.period, tp.file_type
        )
        assert len(cache_files) > 0, "缓存目录中应存在缓存文件"

        cached_data = pd.DataFrame()
        for i in cache_files:
            chunk = read_cache_file(i, tp.file_type)
            cached_data = merge_with_deduplication(cached_data, chunk)

        assert len(cached_data) == count, (
            f"cached_data行数应为 {count}，但实际为 {len(cached_data)}"
        )
        assert_uniform_time_intervals(df_result, "time")
        assert_uniform_time_intervals(cached_data, "time")

    clear_cache_directory(tp.cache_dir)
