import pytest
import polars as pl
from pathlib import Path

from src.cache_tool.storage import read_ohlcv, save_ohlcv
from src.cache_tool.config import get_data_dir
from src.cache_tool.models import DataLocation
from .utils import mock_ohlcv, assert_time_continuous, make_loc


class TestStorage:
    def test_first_write(self, temp_dir, sample_loc):
        """首次写入应创建数据文件"""
        data = mock_ohlcv(start=1000000, count=10)

        save_ohlcv(temp_dir, sample_loc, data)

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

    def test_read_after_write(self, temp_dir, sample_loc):
        """写入后能正确读取"""
        data = mock_ohlcv(start=1000000, count=10)

        save_ohlcv(temp_dir, sample_loc, data)
        result = read_ohlcv(temp_dir, sample_loc)

        assert len(result) == 10
        assert result["time"].to_list() == data["time"].to_list()

    def test_append_continuous(self, temp_dir, sample_loc, period_ms):
        """追加连续数据应合并去重"""
        data1 = mock_ohlcv(start=1000000, count=10, period_ms=period_ms)
        # data2 首条与 data1 尾条重叠
        data2 = mock_ohlcv(start=1000000 + 9 * period_ms, count=10, period_ms=period_ms)

        save_ohlcv(temp_dir, sample_loc, data1)
        save_ohlcv(temp_dir, sample_loc, data2)

        result = read_ohlcv(temp_dir, sample_loc)

        # 10 + 10 - 1(重叠) = 19
        assert len(result) == 19
        assert_time_continuous(result, period_ms)

    def test_read_with_filter(self, temp_dir, sample_loc, period_ms):
        """按时间范围过滤读取"""
        data = mock_ohlcv(start=1000000, count=100, period_ms=period_ms)
        save_ohlcv(temp_dir, sample_loc, data)

        start = 1000000 + 20 * period_ms
        end = 1000000 + 50 * period_ms

        result = read_ohlcv(temp_dir, sample_loc, start, end)

        assert len(result) == 31  # 20到50，包含两端
        assert result["time"].min() == start
        assert result["time"].max() == end

    def test_duplicate_write_idempotent(self, temp_dir, sample_loc):
        """重复写入相同数据应幂等"""
        data = mock_ohlcv(start=1000000, count=10)

        save_ohlcv(temp_dir, sample_loc, data)
        save_ohlcv(temp_dir, sample_loc, data)
        save_ohlcv(temp_dir, sample_loc, data)

        result = read_ohlcv(temp_dir, sample_loc)
        assert len(result) == 10

    def test_empty_data_no_op(self, temp_dir, sample_loc):
        """空数据不应创建文件"""
        data = pl.DataFrame()
        save_ohlcv(temp_dir, sample_loc, data)

        data_dir = get_data_dir(
            temp_dir,
            sample_loc.exchange,
            sample_loc.mode,
            sample_loc.market,
            sample_loc.symbol,
            sample_loc.period,
        )
        # 目录可能由之前的测试创建，如果完全是空的，可能不存在
        # 如果不存在，那也没问题。如果存在，必须没有 .parquet
        if data_dir.exists():
            assert len(list(data_dir.glob("*.parquet"))) == 0

    def test_different_modes_isolated(self, temp_dir):
        """不同 mode 的数据应隔离"""
        loc_live = make_loc(mode="live")
        loc_demo = make_loc(mode="demo")

        data_live = mock_ohlcv(start=1000000, count=10)
        data_demo = mock_ohlcv(start=2000000, count=5)

        save_ohlcv(temp_dir, loc_live, data_live)
        save_ohlcv(temp_dir, loc_demo, data_demo)

        result_live = read_ohlcv(temp_dir, loc_live)
        result_demo = read_ohlcv(temp_dir, loc_demo)

        assert len(result_live) == 10
        assert len(result_demo) == 5
        assert result_live["time"].min() != result_demo["time"].min()

    def test_save_with_lock(self, temp_dir, sample_loc):
        """验证 save_ohlcv_with_lock 正常工作"""
        from src.cache_tool.storage import save_ohlcv_with_lock

        data = mock_ohlcv(start=1000000, count=10)
        save_ohlcv_with_lock(temp_dir, sample_loc, data)

        result = read_ohlcv(temp_dir, sample_loc)
        assert len(result) == 10

        # 验证数据目录存在
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

    def test_cross_month_partition(self, temp_dir):
        """验证跨月数据正确分块到不同文件"""
        from datetime import datetime, timezone

        # 使用分钟级周期（按月分块）
        loc = make_loc(period="15m")

        # 2023年1月31日 23:45 和 2023年2月1日 00:00 (UTC)
        jan_end = int(
            datetime(2023, 1, 31, 23, 45, tzinfo=timezone.utc).timestamp() * 1000
        )
        feb_start = int(
            datetime(2023, 2, 1, 0, 0, tzinfo=timezone.utc).timestamp() * 1000
        )

        # 创建跨月数据
        data = pl.DataFrame(
            {
                "time": [jan_end, feb_start],
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [95.0, 96.0],
                "close": [102.0, 103.0],
                "volume": [1000.0, 1001.0],
            }
        )

        save_ohlcv(temp_dir, loc, data)

        data_dir = get_data_dir(
            temp_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
        )

        # 应生成两个分块文件
        parquet_files = sorted(data_dir.glob("*.parquet"))
        assert len(parquet_files) == 2, f"应生成2个分块文件，实际 {len(parquet_files)}"

        file_names = [f.stem for f in parquet_files]
        assert "2023-01" in file_names, "应有 2023-01.parquet"
        assert "2023-02" in file_names, "应有 2023-02.parquet"

    def test_decade_partition(self, temp_dir):
        """验证10年分块规则: 2023 -> 2020s, 2030 -> 2030s"""
        from datetime import datetime, timezone

        # 使用日线周期（按10年分块）
        loc = make_loc(period="1d")

        # 2023年和2030年的时间戳
        ts_2023 = int(datetime(2023, 6, 15, tzinfo=timezone.utc).timestamp() * 1000)
        ts_2030 = int(datetime(2030, 6, 15, tzinfo=timezone.utc).timestamp() * 1000)

        data = pl.DataFrame(
            {
                "time": [ts_2023, ts_2030],
                "open": [100.0, 200.0],
                "high": [105.0, 205.0],
                "low": [95.0, 195.0],
                "close": [102.0, 202.0],
                "volume": [1000.0, 2000.0],
            }
        )

        save_ohlcv(temp_dir, loc, data)

        data_dir = get_data_dir(
            temp_dir, loc.exchange, loc.mode, loc.market, loc.symbol, loc.period
        )

        parquet_files = sorted(data_dir.glob("*.parquet"))
        assert len(parquet_files) == 2, f"应生成2个分块文件，实际 {len(parquet_files)}"

        file_names = [f.stem for f in parquet_files]
        assert "2020s" in file_names, "2023年应分块到 2020s.parquet"
        assert "2030s" in file_names, "2030年应分块到 2030s.parquet"
