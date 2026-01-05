# 测试代码示例

## 测试目录结构

```
Test/
  conftest.py              # pytest fixtures
  test_storage.py          # 数据读写测试
  test_log_manager.py      # 日志管理测试
  test_continuity.py       # 连续性验证测试
  test_integration.py      # 集成测试
  utils.py                 # 测试工具函数
```

---

## conftest.py - 测试配置

```python
import pytest
import tempfile
from pathlib import Path
import shutil

@pytest.fixture
def temp_dir():
    """为每个测试提供临时目录"""
    temp = tempfile.mkdtemp()
    yield Path(temp)
    shutil.rmtree(temp, ignore_errors=True)

@pytest.fixture
def sample_period():
    return "15m"

@pytest.fixture
def period_ms():
    return 15 * 60 * 1000  # 15分钟 = 900000ms
```

---

## utils.py - 测试工具

```python
import polars as pl

def mock_ohlcv(start: int, count: int, period_ms: int = 900000) -> pl.DataFrame:
    """生成模拟 OHLCV 数据"""
    return pl.DataFrame({
        "time": [start + i * period_ms for i in range(count)],
        "open": [100.0 + i for i in range(count)],
        "high": [105.0 + i for i in range(count)],
        "low": [95.0 + i for i in range(count)],
        "close": [102.0 + i for i in range(count)],
        "volume": [1000.0 + i for i in range(count)],
    })

def assert_time_continuous(df: pl.DataFrame, period_ms: int):
    """断言时间序列连续"""
    if len(df) < 2:
        return
    diffs = df["time"].diff().drop_nulls()
    assert diffs.n_unique() == 1, f"时间不连续: {diffs.unique()}"
    assert diffs[0] == period_ms, f"时间间隔错误: {diffs[0]} != {period_ms}"
```

---

## test_storage.py - 数据读写测试

```python
import pytest
import polars as pl
from pathlib import Path

from src.cache_tool.storage import read_ohlcv, save_ohlcv
from src.cache_tool.config import get_data_dir
from .utils import mock_ohlcv, assert_time_continuous

class TestStorage:
    
    def test_first_write(self, temp_dir):
        """首次写入应创建数据文件"""
        data = mock_ohlcv(start=1000000, count=10)
        
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        
        data_dir = get_data_dir(temp_dir, "BTC/USDT", "15m")
        assert data_dir.exists()
        assert len(list(data_dir.glob("*.parquet"))) >= 1
    
    def test_read_after_write(self, temp_dir):
        """写入后能正确读取"""
        data = mock_ohlcv(start=1000000, count=10)
        
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        result = read_ohlcv(temp_dir, "BTC/USDT", "15m")
        
        assert len(result) == 10
        assert result["time"].to_list() == data["time"].to_list()
    
    def test_append_continuous(self, temp_dir, period_ms):
        """追加连续数据应合并去重"""
        data1 = mock_ohlcv(start=1000000, count=10, period_ms=period_ms)
        # data2 首条与 data1 尾条重叠
        data2 = mock_ohlcv(
            start=1000000 + 9 * period_ms, 
            count=10, 
            period_ms=period_ms
        )
        
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data1)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data2)
        
        result = read_ohlcv(temp_dir, "BTC/USDT", "15m")
        
        # 10 + 10 - 1(重叠) = 19
        assert len(result) == 19
        assert_time_continuous(result, period_ms)
    
    def test_read_with_filter(self, temp_dir, period_ms):
        """按时间范围过滤读取"""
        data = mock_ohlcv(start=1000000, count=100, period_ms=period_ms)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        
        start = 1000000 + 20 * period_ms
        end = 1000000 + 50 * period_ms
        
        result = read_ohlcv(temp_dir, "BTC/USDT", "15m", start, end)
        
        assert len(result) == 31  # 20到50，包含两端
        assert result["time"].min() == start
        assert result["time"].max() == end
    
    def test_duplicate_write_idempotent(self, temp_dir):
        """重复写入相同数据应幂等"""
        data = mock_ohlcv(start=1000000, count=10)
        
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        
        result = read_ohlcv(temp_dir, "BTC/USDT", "15m")
        assert len(result) == 10
    
    def test_empty_data_no_op(self, temp_dir):
        """空数据不应创建文件"""
        data = pl.DataFrame()
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        
        data_dir = get_data_dir(temp_dir, "BTC/USDT", "15m")
        assert not data_dir.exists() or len(list(data_dir.glob("*.parquet"))) == 0
```

---

## test_log_manager.py - 日志管理测试

```python
import pytest
from pathlib import Path

from src.cache_tool.log_manager import (
    append_log, read_log, compact_log, rebuild_log_from_data
)
from src.cache_tool.config import get_data_dir
from src.cache_tool.storage import save_ohlcv
from .utils import mock_ohlcv

class TestLogManager:
    
    def test_append_and_read_log(self, temp_dir):
        """追加日志后能正确读取"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)
        
        log = read_log(data_dir)
        
        assert len(log) == 2
        assert log["data_start"].to_list() == [1000, 2000]
        assert log["data_end"].to_list() == [2000, 3000]
    
    def test_compact_log_merges_continuous(self, temp_dir):
        """合并连续的日志条目"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        # 三条连续日志
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)
        append_log(data_dir, 3000, 4000, 10)
        
        compact_log(data_dir)
        log = read_log(data_dir)
        
        # 应合并为一条
        assert len(log) == 1
        assert log["data_start"][0] == 1000
        assert log["data_end"][0] == 4000
    
    def test_compact_log_preserves_gaps(self, temp_dir):
        """合并时保留断裂点"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)  # 连续
        append_log(data_dir, 4000, 5000, 10)  # 断裂
        append_log(data_dir, 5000, 6000, 10)  # 连续
        
        compact_log(data_dir)
        log = read_log(data_dir)
        
        # 应合并为两条
        assert len(log) == 2
        assert log["data_start"].to_list() == [1000, 4000]
        assert log["data_end"].to_list() == [3000, 6000]
    
    def test_rebuild_log_from_continuous_data(self, temp_dir, period_ms):
        """从连续数据重建日志"""
        data = mock_ohlcv(start=1000000, count=100, period_ms=period_ms)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data)
        
        data_dir = get_data_dir(temp_dir, "BTC/USDT", "15m")
        rebuild_log_from_data(data_dir, period_ms)
        
        log = read_log(data_dir)
        
        # 连续数据应只有一条日志
        assert len(log) == 1
        assert log["data_start"][0] == 1000000
        assert log["data_end"][0] == 1000000 + 99 * period_ms
    
    def test_rebuild_log_detects_gaps(self, temp_dir, period_ms):
        """从有断裂的数据重建日志"""
        # 两段不连续的数据
        data1 = mock_ohlcv(start=1000000, count=10, period_ms=period_ms)
        data2 = mock_ohlcv(start=2000000, count=10, period_ms=period_ms)
        
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data1)
        save_ohlcv(temp_dir, "BTC/USDT", "15m", data2)
        
        data_dir = get_data_dir(temp_dir, "BTC/USDT", "15m")
        rebuild_log_from_data(data_dir, period_ms)
        
        log = read_log(data_dir)
        
        # 应有两条日志
        assert len(log) == 2
```

---

## test_continuity.py - 连续性验证测试

```python
import pytest
from pathlib import Path

from src.cache_tool.continuity import (
    check_continuity, get_data_range, find_missing_ranges
)
from src.cache_tool.log_manager import append_log

class TestContinuity:
    
    def test_no_gaps_when_continuous(self, temp_dir):
        """连续日志无断裂"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 3000, 10)
        append_log(data_dir, 3000, 4000, 10)
        
        gaps = check_continuity(data_dir)
        assert gaps == []
    
    def test_detect_single_gap(self, temp_dir):
        """检测单个断裂"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 3000, 4000, 10)  # 缺失 2000-3000
        
        gaps = check_continuity(data_dir)
        
        assert len(gaps) == 1
        assert gaps[0]["gap_after"] == 2000
        assert gaps[0]["gap_before"] == 3000
    
    def test_detect_multiple_gaps(self, temp_dir):
        """检测多个断裂"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 4000, 5000, 10)  # gap 1
        append_log(data_dir, 7000, 8000, 10)  # gap 2
        
        gaps = check_continuity(data_dir)
        assert len(gaps) == 2
    
    def test_get_data_range(self, temp_dir):
        """获取数据时间范围"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 2000, 10)
        append_log(data_dir, 2000, 5000, 30)
        
        range_info = get_data_range(data_dir)
        
        assert range_info["start"] == 1000
        assert range_info["end"] == 5000
    
    def test_find_missing_ranges_no_data(self, temp_dir):
        """无数据时全部缺失"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        missing = find_missing_ranges(data_dir, 1000, 5000)
        
        assert len(missing) == 1
        assert missing[0]["start"] == 1000
        assert missing[0]["end"] == 5000
    
    def test_find_missing_before_existing(self, temp_dir):
        """检测已有数据之前的缺失"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 3000, 5000, 20)
        
        missing = find_missing_ranges(data_dir, 1000, 5000)
        
        assert len(missing) == 1
        assert missing[0]["start"] == 1000
        assert missing[0]["end"] == 3000
    
    def test_find_missing_after_existing(self, temp_dir):
        """检测已有数据之后的缺失"""
        data_dir = temp_dir / "BTC_USDT_15m"
        data_dir.mkdir(parents=True)
        
        append_log(data_dir, 1000, 3000, 20)
        
        missing = find_missing_ranges(data_dir, 1000, 5000)
        
        assert len(missing) == 1
        assert missing[0]["start"] == 3000
        assert missing[0]["end"] == 5000
```

---

## test_integration.py - 集成测试

```python
import pytest
import polars as pl
from pathlib import Path

from src.cache_tool.entry import get_ohlcv_with_cache
from src.cache_tool.continuity import check_continuity
from src.cache_tool.config import get_data_dir
from .utils import mock_ohlcv, assert_time_continuous

def mock_fetch(symbol, period, start_time, count, **kwargs):
    """模拟 API 获取"""
    period_ms = 15 * 60 * 1000
    return mock_ohlcv(start_time, count, period_ms)

class TestIntegration:
    
    def test_full_workflow(self, temp_dir, period_ms):
        """完整工作流：首次获取 → 追加 → 读取"""
        # 首次获取
        result1 = get_ohlcv_with_cache(
            temp_dir, "BTC/USDT", "15m",
            start_time=1000000,
            count=50,
            fetch_callback=mock_fetch,
        )
        
        assert len(result1) == 50
        assert_time_continuous(result1, period_ms)
        
        # 追加获取（连续）
        result2 = get_ohlcv_with_cache(
            temp_dir, "BTC/USDT", "15m",
            start_time=1000000 + 49 * period_ms,
            count=50,
            fetch_callback=mock_fetch,
        )
        
        # 检查连续性
        data_dir = get_data_dir(temp_dir, "BTC/USDT", "15m")
        gaps = check_continuity(data_dir)
        assert gaps == [], f"发现断裂: {gaps}"
    
    def test_cache_hit(self, temp_dir, period_ms):
        """缓存命中时不应调用 fetch"""
        call_count = {"value": 0}
        
        def counting_fetch(symbol, period, start_time, count, **kwargs):
            call_count["value"] += 1
            return mock_ohlcv(start_time, count, period_ms)
        
        # 首次获取
        get_ohlcv_with_cache(
            temp_dir, "BTC/USDT", "15m",
            start_time=1000000, count=50,
            fetch_callback=counting_fetch,
        )
        
        first_call_count = call_count["value"]
        
        # 再次获取相同范围
        get_ohlcv_with_cache(
            temp_dir, "BTC/USDT", "15m",
            start_time=1000000, count=50,
            fetch_callback=counting_fetch,
        )
        
        # 不应有新的 API 调用
        assert call_count["value"] == first_call_count
```
