# ================
# File: Test\test_handle_cache.py
# ================

import pandas as pd
import pytest
from pathlib import Path
import shutil
from src.cache_tool.cache_entry import get_ohlcv_with_cache, mock_fetch_ohlcv
from src.cache_tool.cache_utils import (
    parse_timestamp_string,
    get_sorted_cache_files,
    get_file_info,
)
from src.cache_tool.cache_read_chunk import get_next_continuous_cache_chunk
from src.cache_tool.cache_file_io import read_cache_file

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
    # 清理临时目录
    shutil.rmtree(temp_cache_dir, ignore_errors=True)


@dataclass
class CacheTestParams:
    """
    用于 get_ohlcv_with_cache 测试用例的参数集合。
    """

    symbol: str = "BTC/USDT"
    period: str = "15m"
    start_time: int = parse_timestamp_string("20230101T000000Z")
    count: int = 20
    cache_size: int = 5
    page_size: int = 10
    file_type: str = "csv"
    cache_dir: Optional[Path] = None


def test_get_ohlcv_with_cache_write_read(cache_setup):
    """
    测试 get_ohlcv_with_cache 函数的写入和读取逻辑。
    """
    print("\ntest_get_ohlcv_with_cache_write_read")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 第一次调用：正常请求并写入缓存
    print("\n--- 第一次调用: 请求并写入缓存 ---")
    df_write = get_ohlcv_with_cache(**vars(tp), fetch_callback=mock_fetch_ohlcv)

    assert not df_write.empty, "第一次调用返回的DataFrame不应为空"
    assert len(df_write) == tp.count, (
        f"第一次调用返回的DataFrame行数应为 {tp.count}，但实际为 {len(df_write)}"
    )

    # 验证缓存文件是否已创建
    cache_files = list(tp.cache_dir.rglob(f"*.{tp.file_type}"))
    assert len(cache_files) > 0, "缓存目录中应存在缓存文件"


# --- 新增测试：验证缓存文件内容与元数据 ---
def test_cache_verification(cache_setup):
    """
    验证缓存文件的元数据（文件名）与实际内容（DataFrame）是否一致。
    """
    print("\ntest_cache_verification")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 先执行一次函数调用，以确保生成了缓存文件
    print("\n--- 生成测试缓存文件 ---")
    get_ohlcv_with_cache(**vars(tp), fetch_callback=mock_fetch_ohlcv)

    # 获取并验证缓存数据块
    cached_data = get_next_continuous_cache_chunk(
        tp.cache_dir, tp.symbol, tp.period, tp.start_time, tp.count, tp.file_type
    )
    assert len(cached_data) == tp.count, (
        f"缓存数量 {len(cached_data)} 和预期数量 {tp.count} 不一致"
    )
    assert cached_data.iloc[0]["time"] == tp.start_time, (
        f"缓存起始时间 {cached_data.iloc[0]['time']} 和预期起始时间 {tp.start_time} 不一致"
    )

    # 遍历所有缓存文件，验证文件名中的元数据
    print(f"\n--- 验证缓存文件元数据和内容一致性 ---")
    sorted_files = get_sorted_cache_files(
        tp.cache_dir, tp.symbol, tp.period, tp.file_type
    )

    for f in sorted_files:
        df = read_cache_file(f, tp.file_type)
        info = get_file_info(f.name)
        assert info["start_time"] == df.iloc[0]["time"], (
            f"缓存起始时间 {df.iloc[0]['time']} 和缓存名字起始时间 {info['start_time']} 不一致"
        )
        assert info["end_time"] == df.iloc[-1]["time"], (
            f"缓存结束时间 {df.iloc[-1]['time']} 和缓存名字结束时间 {info['end_time']} 不一致"
        )
        assert info["count"] == len(df), (
            f"缓存记录数 {len(df)} 和缓存名字记录数 {info['count']} 不一致"
        )
    print("✅ 所有缓存文件元数据验证通过。")


# --- 测试缓存完全命中 (保持不变) ---
def test_cache_full_hit(cache_setup, capsys):
    """
    测试 get_ohlcv_with_cache 函数在缓存完全命中的情况下，是否正确返回数据且不发起网络请求。
    """
    print("\ntest_cache_full_hit")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 模拟第一次调用，写入缓存
    _ = get_ohlcv_with_cache(**vars(tp), fetch_callback=mock_fetch_ohlcv)

    # 定义一个空的回调函数，以验证第二次调用是否会发起网络请求
    def noop_fetch_callback(*args, **kwargs):
        print("❌ 模拟网络请求被调用，但它不应该被调用！")
        return pd.DataFrame()

    # 捕获并清空之前的日志
    capsys.readouterr()

    # 第二次调用：此时应该从缓存读取，不发起网络请求
    print("\n--- 第二次调用: 从缓存中读取 ---")
    df_read = get_ohlcv_with_cache(
        **vars(tp),
        fetch_callback=noop_fetch_callback,
    )

    # 捕获第二次调用的日志
    captured = capsys.readouterr()

    # 验证结果
    assert not df_read.empty, "返回的DataFrame不应为空"
    assert len(df_read) == tp.count, (
        f"返回的DataFrame行数应为 {tp.count}，但实际为 {len(df_read)}"
    )

    # 验证是否打印了“缓存完全命中”的日志
    assert "✅ 缓存完全命中，直接返回。" in captured.out, "未找到预期的缓存命中日志"

    # 验证是否没有发起网络请求的日志
    assert "ℹ️ 缓存不足或无缓存，开始请求新数据。" not in captured.out


# --- 新增测试：部分缓存命中 ---
def test_get_ohlcv_with_partial_cache_hit(cache_setup, capsys):
    """
    测试 get_ohlcv_with_cache 函数在部分命中缓存时，是否能正确处理。
    """
    print("\ntest_get_ohlcv_with_partial_cache_hit")
    tp = CacheTestParams(cache_dir=cache_setup)

    # 第一次调用：生成包含 20 条数据的缓存
    print("\n--- 第一次调用: 生成 20 条数据的缓存 ---")
    _ = get_ohlcv_with_cache(**vars(tp), fetch_callback=mock_fetch_ohlcv)

    # 定义部分请求的参数：从第 6 条数据开始，请求 15 条
    interval_ms = 15 * 60 * 1000
    partial_start_time = tp.start_time + 15 * interval_ms
    partial_count = 10

    # 捕获并清空之前的日志
    capsys.readouterr()

    # 第二次调用：请求部分数据
    print("\n--- 第二次调用: 请求部分数据 ---")
    df_partial = get_ohlcv_with_cache(
        symbol=tp.symbol,
        period=tp.period,
        start_time=partial_start_time,
        count=partial_count,
        cache_dir=tp.cache_dir,
        cache_size=tp.cache_size,
        page_size=tp.page_size,
        file_type=tp.file_type,
        fetch_callback=mock_fetch_ohlcv,
    )

    # 捕获第二次调用的日志
    captured = capsys.readouterr()

    # 验证返回的数据
    assert not df_partial.empty, "返回的DataFrame不应为空"
    assert len(df_partial) == partial_count, (
        f"返回行数应为 {partial_count}，但实际为 {len(df_partial)}"
    )
    assert df_partial.iloc[0]["time"] == partial_start_time, "返回数据的起始时间不正确"

    # 验证日志输出，确认缓存命中和网络请求都发生了
    assert "✅ 缓存命中" in captured.out, "应该从缓存读取数据"
    assert "ℹ️ 缓存不足或无缓存，开始请求新数据。" in captured.out, (
        "应该发起网络请求以获取缺失数据"
    )
    print("✅ 部分缓存命中测试通过。")
