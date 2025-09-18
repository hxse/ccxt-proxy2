import polars as pl
import pytest
from src.cache_tool.cache_entry import get_ohlcv_with_cache, mock_fetch_ohlcv
from src.cache_tool.cache_utils import (
    parse_timestamp_string,
    get_sorted_cache_files,
    find_cache_size_sequences,
    group_continuous_files,
)
from src.cache_tool.cache_file_io import read_cache_file
from Test.utils import (
    clear_cache_directory,
    assert_uniform_time_intervals,
    validate_merged_data,
)
from src.cache_tool.cache_data_processor import merge_with_deduplication
from Test.conftest import CacheTestParams


@pytest.mark.parametrize("enable_consolidate", [False, True])
@pytest.mark.parametrize(
    "start_times_str",
    [
        ("20230101T060000Z", "20230101T100000Z", "20230101T080000Z"),
        ("20230101T060000Z", "20230101T041500Z", "20230101T080000Z"),
        ("20230101T060000Z", "20230101T041500Z", "20230101T021500Z"),
        ("20230101T021500Z", "20230101T041500Z", "20230101T060000Z"),
    ],
)
def test_get_ohlcv_with_cache_multiple_calls(
    cache_setup, enable_consolidate, start_times_str
):
    print(
        f"\n--- Running test with consolidate={enable_consolidate} and start times: {start_times_str} ---"
    )
    tp = CacheTestParams(cache_dir=cache_setup)
    tp.count = 10  # 确保 count 参数在所有测试中保持一致

    start_times = [parse_timestamp_string(t) for t in start_times_str]
    df_writes = []

    # 动态执行三次调用
    for i, start_time in enumerate(start_times):
        print(f"\n--- 第 {i + 1} 次调用: 请求并写入缓存, start_time={start_time} ---")
        tp.start_time = start_time
        df = get_ohlcv_with_cache(
            **vars(tp),
            fetch_callback=mock_fetch_ohlcv,
            enable_consolidate=enable_consolidate,
        )
        assert len(df) == tp.count, f"df行数应为 {tp.count}，但实际为 {len(df)}"
        print(
            f"df{i + 1}",
            len(df),
            df.head(1)["date"].item(),
            df.tail(1)["date"].item(),
        )
        df_writes.append(df)

    # 合并所有结果
    df_write_merge = pl.DataFrame()
    for df in df_writes:
        df_write_merge = merge_with_deduplication(df_write_merge, df)
    print("总合并后数据行数:", len(df_write_merge))

    # 验证缓存文件是否已创建
    cache_files = get_sorted_cache_files(
        tp.cache_dir, tp.symbol, tp.period, tp.file_type
    )
    assert len(cache_files) > 0, "缓存目录中应存在缓存文件"

    # 从缓存中读取并合并数据
    cached_data = pl.DataFrame()
    for file in cache_files:
        chunk = read_cache_file(file, tp.file_type)
        cached_data = merge_with_deduplication(cached_data, chunk)

    assert len(cached_data) == len(df_write_merge), (
        f"缓存数据行数应为 {len(df_write_merge)}，但实际为 {len(cached_data)}"
    )

    # 运行断言
    validate_merged_data(cached_data, start_times, tp.period, tp.count)
    assert_uniform_time_intervals(df_writes[0], "time")
    assert_uniform_time_intervals(cached_data, "time")

    sorted_cache_files_2d = group_continuous_files(cache_files)

    assert len(sorted_cache_files_2d) == 1, "预期只有一个连续缓存组合"
    cache_size_sequences = find_cache_size_sequences(
        sorted_cache_files_2d[0], tp.cache_size
    )

    if enable_consolidate:
        assert len(cache_size_sequences) == 1, "预期连续cache_size的段落 == 1"
    else:
        assert len(cache_size_sequences) > 1, "预期连续cache_size的段落 > 1"

    clear_cache_directory(tp.cache_dir)
