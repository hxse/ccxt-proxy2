import pytest
from pathlib import Path
from src.cache_tool.cache_utils import parse_timestamp_string
from Test.utils import clear_cache_directory
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
    # 清理临时目录下的所有文件和子目录

    clear_cache_directory(temp_cache_dir)


@dataclass
class CacheTestParams:
    """
    用于 get_ohlcv_with_cache 测试用例的参数集合。
    """

    symbol: str = "BTC/USDT"
    period: str = "15m"
    start_time: int = parse_timestamp_string("20230101T060000Z")
    count: int = 15
    cache_dir: Optional[Path] = None
    cache_size: int = 5
    page_size: int = 10
    enable_cache: bool = True
    file_type: str = "csv"
