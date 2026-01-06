"""边界场景测试"""

import pytest
import polars as pl
from pathlib import Path
import warnings
from typing import cast

from src.cache_tool.entry import get_ohlcv_with_cache
from src.cache_tool.storage import save_ohlcv, read_ohlcv
from src.cache_tool.log_manager import read_log, get_log_path
from src.cache_tool.config import get_data_dir, MAX_PER_REQUEST
from .utils import mock_ohlcv, assert_time_continuous


class TestEdgeCases:
    """边界场景测试"""

    def test_last_candle_price_update(self, temp_dir, sample_loc, period_ms):
        """去重后无新增数据，但最后一根K线价格更新"""
        # 预先写入缓存：t=1000000 开始 9 根 (比请求少1根，触发网络请求)
        pre_data = mock_ohlcv(1000000, 9, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        # 缓存的最后一根 (第9根)
        last_time_in_cache = cast(int, pre_data["time"].max())

        # 验证初始价格
        original_close = pre_data.filter(pl.col("time") == last_time_in_cache)["close"][
            0
        ]

        def fetch_overlapping_update(symbol, period, start_time, count, **kwargs):
            # start_time 应该是 last_time_in_cache (首尾衔接)
            # 返回 2 根：last_time_in_cache (价格更新) 和下一根

            # 构造更新后的第9根
            updated_9 = pl.DataFrame(
                {
                    "time": [last_time_in_cache],
                    "open": [100.0],
                    "high": [200.0],
                    "low": [90.0],
                    "close": [999.9],  # 价格大幅更新
                    "volume": [5000.0],
                }
            )

            # 第10根
            next_time = last_time_in_cache + period_ms
            new_10 = mock_ohlcv(next_time, 1, period_ms)

            return pl.concat([updated_9, new_10])

        # 请求 10 根 (缓存9 + 网络1)
        # 这将触发：
        # 1. 读缓存 1-9
        # 2. 从 9 开始请求网络
        # 3. 网络返回 9(updated), 10
        # 4. 合并 1-9, 9(updated), 10 -> 9被更新
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=fetch_overlapping_update,
        )

        assert len(result) == 10

        # 验证最后一根K线(第9根)是否更新
        # 注意 result 里的第9根 (index 8)
        updated_close = result.filter(pl.col("time") == last_time_in_cache)["close"][0]
        assert updated_close == 999.9, (
            f"缓存重叠部分的K线价格应更新，实际为 {updated_close}"
        )

        # 验证缓存文件也已更新
        cached = read_ohlcv(temp_dir, sample_loc)
        disk_close = cached.filter(pl.col("time") == last_time_in_cache)["close"][0]
        assert disk_close == 999.9, "磁盘缓存也应被更新"

    def test_network_returns_empty(self, temp_dir, sample_loc, period_ms):
        """网络返回空数据时应正确退出，不死循环"""

        def empty_fetch(symbol, period, start_time, count, **kwargs):
            return pl.DataFrame()

        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=empty_fetch,
        )

        assert result.is_empty(), "网络返回空数据时，结果应为空"

    def test_large_request_batching(self, temp_dir, sample_loc, period_ms):
        """请求超过 MAX_PER_REQUEST 时应分批请求"""
        request_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            request_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)

        # 请求 3000 条（超过 MAX_PER_REQUEST=1500）
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=3000,
            fetch_callback=counting_fetch,
        )

        assert len(result) == 3000, f"应返回 3000 条，实际 {len(result)}"
        assert request_count["value"] >= 2, (
            f"应至少分 2 批请求，实际 {request_count['value']} 次"
        )
        assert_time_continuous(result, period_ms)

    def test_corrupted_log_auto_rebuild(self, temp_dir, sample_loc, period_ms):
        """日志损坏时自动警告并重建"""
        # 先写入正常数据
        data = mock_ohlcv(1000000, 50, period_ms)
        save_ohlcv(temp_dir, sample_loc, data)

        data_dir = get_data_dir(
            temp_dir,
            sample_loc.exchange,
            sample_loc.mode,
            sample_loc.market,
            sample_loc.symbol,
            sample_loc.period,
        )

        # 模拟日志损坏：写入无法解析的内容
        log_path = get_log_path(data_dir)
        with open(log_path, "w", encoding="utf-8") as f:
            f.write('{"broken json\n')

        # 读取日志应触发警告并重建
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            log = read_log(data_dir)

            # 应有警告
            assert len(w) >= 1, "应有日志损坏警告"
            assert "日志损坏" in str(w[0].message), (
                f"警告内容应包含'日志损坏'，实际: {w[0].message}"
            )

        # 重建后日志应正常
        assert len(log) == 1, f"重建后应有 1 条日志，实际 {len(log)}"
        assert log[0].data_start == 1000000, "重建后日志 data_start 应为 1000000"
        assert log[0].count == 50, f"重建后日志 count 应为 50，实际 {log[0].count}"

    def test_dedup_no_new_data_still_saves(self, temp_dir, sample_loc, period_ms):
        """去重后无新增数据时，仍应保存（测试网络仅返回重叠数据的情况）"""
        # 预先写入 9 根
        pre_data = mock_ohlcv(1000000, 9, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        last_time_in_cache = cast(int, pre_data["time"].max())

        # 请求 9 根 (但在网络请求里只返回第9根的更新，没有第10根)
        # 这模拟了：网络也没有更多新数据了，但是第9根更新了

        def fetch_just_overlap_update(symbol, period, start_time, count, **kwargs):
            # 仅返回重叠的第9根，且价格更新
            return pl.DataFrame(
                {
                    "time": [last_time_in_cache],
                    "open": [100.0],
                    "high": [200.0],
                    "low": [90.0],
                    "close": [888.8],  # 更新
                    "volume": [5000.0],
                }
            )

        # 请求 10 根 -> 读缓存 9 根 -> 缺 1 根 -> 请求网络
        # 网络只返回了已有的第9根(updated)
        # 合并后长度仍为 9 (没有增加)
        # 此时应触发 break (prev_len == len)，但仍需保存
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=fetch_just_overlap_update,
        )

        assert len(result) == 9, "结果长度应仍为 9"

        # 验证价格更新被保存
        cached = read_ohlcv(temp_dir, sample_loc)
        updated_close = cached.filter(pl.col("time") == last_time_in_cache)["close"][0]

        assert updated_close == 888.8, "即使没有新K线增加，已有K线的更新也应被保存"
