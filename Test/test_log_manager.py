import pytest

from src.cache_tool.log_manager import (
    CorruptedProofLogError,
    append_log,
    read_log,
    compact_log,
)
from src.cache_tool.config import get_data_dir
from src.cache_tool.storage import save_ohlcv
from .utils import mock_ohlcv


class TestLogManager:
    def test_append_and_read_log(self, temp_dir):
        """追加日志后能正确读取"""
        data_dir = temp_dir / "binance" / "live" / "future" / "BTC_USDT" / "15m"
        data_dir.mkdir(parents=True)

        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)

        log = read_log(data_dir)

        assert len(log) == 2
        assert log[0].data_start == 1000
        assert log[1].data_start == 2000

    def test_compact_log_merges_continuous(self, temp_dir):
        """合并连续的日志条目"""
        data_dir = temp_dir / "binance" / "live" / "future" / "BTC_USDT" / "15m"
        data_dir.mkdir(parents=True)

        # 三条连续日志
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)
        append_log(data_dir, 3000, 4000, 10)

        compact_log(data_dir)
        log = read_log(data_dir)

        # 应合并为一条
        assert len(log) == 1
        assert log[0].data_start == 1000
        assert log[0].data_end == 4000

    def test_compact_log_merges_overlapping(self, temp_dir):
        """合并重叠的日志条目"""
        data_dir = temp_dir / "binance" / "live" / "future" / "BTC_USDT" / "15m"
        data_dir.mkdir(parents=True)

        # 重叠日志
        append_log(data_dir, 1000, 3000, 20)
        append_log(data_dir, 2000, 4000, 20)  # 与上一条重叠

        compact_log(data_dir)
        log = read_log(data_dir)

        # 应合并为一条
        assert len(log) == 1
        assert log[0].data_start == 1000
        assert log[0].data_end == 4000

    def test_compact_log_preserves_gaps(self, temp_dir):
        """合并时保留断裂点"""
        data_dir = temp_dir / "binance" / "live" / "future" / "BTC_USDT" / "15m"
        data_dir.mkdir(parents=True)

        append_log(data_dir, 1000, 2000, 10)
        # Gap here (3000 starts)
        append_log(data_dir, 3000, 4000, 10)

        compact_log(data_dir)
        log = read_log(data_dir)

        # Should stay 2
        assert len(log) == 2
        assert log[0].data_end == 2000
        assert log[1].data_start == 3000

    def test_missing_log_returns_empty(self, temp_dir, sample_loc, period_ms):
        """日志缺失时返回空列表，不做自动恢复。"""
        data = mock_ohlcv(start=1000000, count=100, period_ms=period_ms)
        save_ohlcv(temp_dir, sample_loc, data)

        data_dir = get_data_dir(
            temp_dir,
            sample_loc.exchange,
            sample_loc.mode,
            sample_loc.market,
            sample_loc.symbol,
            sample_loc.period,
        )

        # Delete log
        log_path = data_dir / "fetch_log.jsonl"
        log_path.unlink()

        log = read_log(data_dir)
        assert log == []

    def test_corrupted_log_raises(self, temp_dir):
        """日志损坏时显式报错，不做自动恢复。"""
        data_dir = temp_dir / "binance" / "live" / "future" / "BTC_USDT" / "15m"
        data_dir.mkdir(parents=True)

        log_path = data_dir / "fetch_log.jsonl"
        log_path.write_text('{"broken json\n', encoding="utf-8")

        with pytest.raises(CorruptedProofLogError):
            read_log(data_dir)
