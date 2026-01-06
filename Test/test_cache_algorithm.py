import pytest
import polars as pl
from pathlib import Path

from src.cache_tool.entry import get_ohlcv_with_cache
from src.cache_tool.storage import save_ohlcv
from src.cache_tool.log_manager import append_log, compact_log
from src.cache_tool.config import get_data_dir
from src.cache_tool.models import DataLocation
from .utils import mock_ohlcv, assert_time_continuous


def mock_fetch(symbol, period, start_time, count, **kwargs):
    """模拟 API 获取"""
    period_ms = 15 * 60 * 1000
    if start_time is None:
        # 默认返回最新，假设是 5000000
        start_time = 5000000
    return mock_ohlcv(start_time, count, period_ms)


class TestCacheAlgorithmSimple:
    """简化缓存算法专项测试"""

    def test_start_in_cache_reuse(self, temp_dir, sample_loc, period_ms):
        """起始时间在缓存中，应复用缓存"""
        # 预先写入缓存数据 t=1000000 开始 20 根
        pre_data = mock_ohlcv(1000000, 20, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        call_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)

        # 请求 t=1000000 + 5*period 开始，10 根
        # 起始在缓存中(1000000 到 1000000+19*period)
        start = 1000000 + 5 * period_ms
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=start,
            count=10,
            fetch_callback=counting_fetch,
        )

        assert len(result) == 10
        # 应该完全从缓存读取，不发起网络请求
        assert call_count["value"] == 0

    def test_start_not_in_cache(self, temp_dir, sample_loc, period_ms):
        """起始时间不在缓存中，应发起网络请求"""
        # 预先写入缓存数据 t=2000000 开始 20 根
        pre_data = mock_ohlcv(2000000, 20, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        call_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)

        # 请求 t=1000000 开始，10 根（不在缓存 2000000-xxx 中）
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=counting_fetch,
        )

        assert len(result) == 10
        # 应该发起网络请求
        assert call_count["value"] >= 1

    def test_partial_cache_hit(self, temp_dir, sample_loc, period_ms):
        """部分缓存命中：起始在缓存中，但需要更多数据"""
        # 预先写入缓存数据 t=1000000 开始 10 根
        pre_data = mock_ohlcv(1000000, 10, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        call_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            # 注意: fetch callback 的 start_time 是由算法决定的，会是 1000000 + 10*period?
            # 简化算法读取缓存后，current_time = cache_end (last time)
            # 下一次 fetch 从 current_time 开始 (即 heavy overlap, but keeps last)
            # 或者 +1?
            # 实际上 entry.py 里的逻辑是 current_time = cache_entry.data_end
            # 所以 fetch 从 data_end 开始。
            # mock_ohlcv(data_end, ...)
            return mock_ohlcv(start_time, count, period_ms)

        # 请求 t=1000000 开始，20 根（缓存只有 10 根）
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=20,
            fetch_callback=counting_fetch,
        )

        assert len(result) == 20
        assert_time_continuous(result, period_ms)
        # 应该发起网络请求获取剩余数据
        assert call_count["value"] >= 1

    def test_no_start_time_skip_cache_read(self, temp_dir, sample_loc, period_ms):
        """无起始时间时跳过缓存读取，只写入"""
        # 预先写入缓存数据
        pre_data = mock_ohlcv(1000000, 10, period_ms)
        save_ohlcv(temp_dir, sample_loc, pre_data)

        call_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            # 返回模拟的"最新"数据
            return mock_ohlcv(5000000, count, period_ms)

        # 无起始时间请求
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=None,
            count=10,
            fetch_callback=counting_fetch,
        )

        assert len(result) == 10
        # 必须发起网络请求（跳过缓存读取）
        assert call_count["value"] == 1

    def test_network_returns_less_than_requested(self, temp_dir, sample_loc, period_ms):
        """网络返回数据不足时应正确退出"""

        def partial_fetch(symbol, period, start_time, count, **kwargs):
            # 只返回 5 根，无论请求多少
            return mock_ohlcv(start_time, min(5, count), period_ms)

        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=20,
            fetch_callback=partial_fetch,
        )

        # 应该正确退出，返回已获取的数据
        assert len(result) == 5

    def test_middle_cache_not_reused(self, temp_dir, sample_loc, period_ms):
        """简化算法不复用中间缓存（与完整算法的区别）"""
        # 预先写入两段缓存：t=1000000-10根，t=2000000-10根
        data1 = mock_ohlcv(1000000, 10, period_ms)
        data2 = mock_ohlcv(2000000, 10, period_ms)
        save_ohlcv(temp_dir, sample_loc, data1)
        save_ohlcv(temp_dir, sample_loc, data2)

        request_starts = []

        def tracking_fetch(symbol, period, start_time, count, **kwargs):
            request_starts.append(start_time)
            return mock_ohlcv(start_time, count, period_ms)

        # 请求 t=500000 开始，覆盖到 2000000 之后
        # 中间的 2000000-xxx 缓存不会被复用（简化算法特性）
        result = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=500000,
            count=30,
            fetch_callback=tracking_fetch,
        )

        # 起始不在缓存中，所以会从 500000 开始连续请求
        # 中间经过 2000000 时不会复用缓存
        assert len(request_starts) >= 1
        assert request_starts[0] == 500000

    def test_enable_cache_false(self, temp_dir, sample_loc, period_ms):
        """禁用缓存时数据不写入磁盘"""
        from src.cache_tool.config import get_data_dir

        call_count = {"value": 0}

        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)

        # 第一次请求，禁用缓存
        result1 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=counting_fetch,
            enable_cache=False,
        )

        assert len(result1) == 10
        assert call_count["value"] == 1

        # 验证没有写入磁盘
        data_dir = get_data_dir(
            temp_dir,
            sample_loc.exchange,
            sample_loc.mode,
            sample_loc.market,
            sample_loc.symbol,
            sample_loc.period,
        )
        parquet_files = list(data_dir.glob("*.parquet")) if data_dir.exists() else []
        assert len(parquet_files) == 0, "禁用缓存时不应写入 parquet 文件"

        # 第二次相同请求，应再次触发网络请求
        result2 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=10,
            fetch_callback=counting_fetch,
            enable_cache=False,
        )

        assert len(result2) == 10
        assert call_count["value"] == 2, "禁用缓存时每次都应发起网络请求"
