import sys

from pathlib import Path


root_path = next(
    (p for p in Path(__file__).resolve().parents if (p / "pyproject.toml").is_file()),
    None,
)

if root_path:
    sys.path.insert(0, str(root_path))


import pytest

from unittest.mock import patch

import pandas as pd

from pathlib import Path

import shutil

from src.router.trader_router import get_ohlcv

from src.cache_tool.handle_cache import (
    sanitize_symbol,
    _parse_timestamp_string,
)


# 定义一个临时的缓存目录，用于测试

TEST_CACHE_DIR = Path("./test_database")


@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_cache():
    """在模块级清理测试缓存目录，只运行一次。"""

    if TEST_CACHE_DIR.exists():
        shutil.rmtree(TEST_CACHE_DIR)

    TEST_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    yield

    if TEST_CACHE_DIR.exists():
        shutil.rmtree(TEST_CACHE_DIR)


@pytest.fixture
def mock_dependencies():
    """

    一个 fixture，用于模拟所有外部依赖，并在测试结束后自动清理。

    """

    with (
        # 模拟 verify_token，防止它被调用
        patch("src.tools.shared.verify_token", return_value=True),
        patch("src.router.trader_router.config", {"market_type": "spot"}),
        patch("src.tools.shared.binance_exchange"),
        patch("src.tools.shared.kraken_exchange"),
    ):
        yield


def test_get_ohlcv_with_cache(mock_dependencies):
    """

    直接调用 get_ohlcv 函数，并使用 mock 模拟所有外部依赖。

    """

    # 准备测试参数

    params = {
        "exchange_name": "binance",
        "symbol": "BTC/USDT",
        "period": "1m",
        "start_time": _parse_timestamp_string("20230101T000000Z"),
        "count": 10,
        "enable_cache": True,
        "enable_test": True,
        "cache_dir": str(TEST_CACHE_DIR),
    }

    # 直接调用路由函数
    ohlcv_data = get_ohlcv(**params)

    # 验证返回的数据

    assert isinstance(ohlcv_data, list)

    assert len(ohlcv_data) == params["count"]

    assert len(ohlcv_data[0]) == 6

    assert ohlcv_data[0][0] == params["start_time"]

    # 验证缓存文件是否被创建

    sanitized_symbol = sanitize_symbol(params["symbol"])

    expected_cache_sub_dir = (
        TEST_CACHE_DIR
        / params["exchange_name"]
        / "spot"
        / sanitized_symbol
        / params["period"]
    )

    assert expected_cache_sub_dir.exists(), (
        f"缓存目录 {expected_cache_sub_dir} 未被创建"
    )

    cache_files = list(expected_cache_sub_dir.glob("*.parquet"))

    assert len(cache_files) > 0, "缓存文件未被创建"

    # 验证缓存文件内容

    cached_df = pd.read_parquet(cache_files[0])

    assert not cached_df.empty, "缓存文件内容为空"

    # 验证文件名信息

    file_info = cache_files[0].stem.split(" ")

    assert file_info[0] == sanitized_symbol

    assert file_info[1] == params["period"]
