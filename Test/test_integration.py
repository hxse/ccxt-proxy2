from datetime import datetime, timezone
from src.cache_tool.entry import get_ohlcv_with_cache
from src.cache_tool.config import get_data_dir
from .utils import mock_ohlcv, mock_ohlcv_from_times, assert_time_continuous


class TestIntegration:
    """集成测试"""

    def test_full_workflow(self, temp_dir, sample_loc, period_ms):
        """完整工作流测试：请求 -> 缓存 -> 再次请求(命中缓存)"""

        # 1. 第一次请求：无缓存，全部网络获取
        def fetch_full(symbol, period, start_time, count, **kwargs):
            return mock_ohlcv(start_time, count, period_ms)

        result1 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=20,
            fetch_callback=fetch_full,
        )
        assert len(result1) == 20
        assert_time_continuous(result1, period_ms)

        # 2. 第二次请求：部分命中，请求后半部分
        call_count = {"value": 0}

        def fetch_partial(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)

        result2 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=1000000,
            count=30,  # 多10根
            fetch_callback=fetch_partial,
        )
        assert len(result2) == 30
        assert_time_continuous(result2, period_ms)

        # 应该只请求了后半部分
        # 0-19 在缓存，20-29 网络获取
        # 简化算法会从缓存末尾开始请求
        assert call_count["value"] >= 1

        # 验证文件是否生成
        data_dir = get_data_dir(
            temp_dir,
            sample_loc.exchange,
            sample_loc.mode,
            sample_loc.market,
            sample_loc.symbol,
            sample_loc.period,
        )
        assert data_dir.exists()
        assert len(list(data_dir.glob("*.parquet"))) >= 1

    def test_market_closure_chain_uses_overlap_instead_of_period_prediction(
        self, temp_dir, sample_loc
    ):
        """存在自然休盘时，算法通过首尾重叠续接数据，而不是预测下一根时间。"""
        jan_2330 = int(
            datetime(2024, 1, 31, 23, 30, tzinfo=timezone.utc).timestamp() * 1000
        )
        jan_2345 = int(
            datetime(2024, 1, 31, 23, 45, tzinfo=timezone.utc).timestamp() * 1000
        )
        feb_open = int(
            datetime(2024, 2, 5, 9, 30, tzinfo=timezone.utc).timestamp() * 1000
        )
        feb_next = int(
            datetime(2024, 2, 5, 9, 45, tzinfo=timezone.utc).timestamp() * 1000
        )

        def fetch_initial(symbol, period, start_time, count, **kwargs):
            return mock_ohlcv_from_times([jan_2330, jan_2345, feb_open])

        result_1 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=jan_2330,
            count=3,
            fetch_callback=fetch_initial,
        )
        assert result_1["time"].to_list() == [jan_2330, jan_2345, feb_open]

        call_args: list[tuple[int | None, int]] = []

        def fetch_follow_up(symbol, period, start_time, count, **kwargs):
            call_args.append((start_time, count))
            return mock_ohlcv_from_times([jan_2345, feb_open, feb_next])

        result_2 = get_ohlcv_with_cache(
            temp_dir,
            sample_loc,
            start_time=jan_2330,
            count=4,
            fetch_callback=fetch_follow_up,
        )

        assert result_2["time"].to_list() == [jan_2330, jan_2345, feb_open, feb_next]
        assert call_args == [(jan_2345, 3)]
