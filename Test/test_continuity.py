import pytest
from src.cache_tool.continuity import check_continuity, find_missing_ranges
from src.cache_tool.log_manager import append_log
from .utils import make_loc


class TestContinuity:
    def test_check_continuity_no_gaps(self, temp_dir):
        """数据连续时无断裂"""
        data_dir = temp_dir / "test"
        data_dir.mkdir()

        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)

        gaps = check_continuity(data_dir)
        assert len(gaps) == 0

    def test_check_continuity_with_gap(self, temp_dir):
        """数据断裂时能检测出断裂"""
        data_dir = temp_dir / "test"
        data_dir.mkdir()

        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 3000, 4000, 10)  # Gap 2000-3000

        gaps = check_continuity(data_dir)
        assert len(gaps) == 1
        assert gaps[0].gap_after == 2000
        assert gaps[0].gap_before == 3000

    def test_find_missing_ranges_complete_miss(self, temp_dir):
        """完全缺失"""
        data_dir = temp_dir / "test"
        data_dir.mkdir()

        # Logs empty
        missing = find_missing_ranges(data_dir, 1000, 5000)
        assert len(missing) == 1
        assert missing[0].start == 1000
        assert missing[0].end == 5000

    def test_find_missing_ranges_partial(self, temp_dir):
        """部分缺失 + 中间断裂"""
        data_dir = temp_dir / "test"
        data_dir.mkdir()

        append_log(data_dir, 2000, 3000, 10)
        append_log(data_dir, 4000, 5000, 10)

        # Target: 1000 -> 6000
        # Expected missing:
        # 1. 1000 -> 2000 (pre)
        # 2. 3000 -> 4000 (gap)
        # 3. 5000 -> 6000 (post)

        missing = find_missing_ranges(data_dir, 1000, 6000)
        assert len(missing) == 3
        assert missing[0].start == 1000
        assert missing[0].end == 2000
        assert missing[1].start == 3000
        assert missing[1].end == 4000
        assert missing[2].start == 5000
        assert missing[2].end == 6000
