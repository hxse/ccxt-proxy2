import pytest
import tempfile
from pathlib import Path
import shutil

from src.cache_tool.models import DataLocation


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


@pytest.fixture
def sample_loc():
    """标准测试用的 DataLocation"""
    return DataLocation(
        exchange="binance",
        mode="live",
        market="future",
        symbol="BTC/USDT",
        period="15m",
    )
