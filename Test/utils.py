import polars as pl
from typing import cast
from typing import Literal
from src.cache_tool.models import DataLocation, VALID_PERIODS


def mock_ohlcv(start: int, count: int, period_ms: int = 900000) -> pl.DataFrame:
    """生成模拟 OHLCV 数据"""
    return pl.DataFrame(
        {
            "time": [start + i * period_ms for i in range(count)],
            "open": [100.0 + i for i in range(count)],
            "high": [105.0 + i for i in range(count)],
            "low": [95.0 + i for i in range(count)],
            "close": [102.0 + i for i in range(count)],
            "volume": [1000.0 + i for i in range(count)],
        }
    )


def assert_time_continuous(df: pl.DataFrame, period_ms: int):
    """断言时间序列连续"""
    if len(df) < 2:
        return
    diffs = df["time"].diff().drop_nulls()
    assert diffs.n_unique() == 1, f"时间不连续: {diffs.unique()}"
    assert diffs[0] == period_ms, f"时间间隔错误: {diffs[0]} != {period_ms}"


# ...


def make_loc(
    exchange: str = "binance",
    mode: str = "live",
    market: str = "future",
    symbol: str = "BTC/USDT",
    period: str = "15m",
) -> DataLocation:
    """快速创建 DataLocation"""
    return DataLocation(
        exchange=exchange,
        mode=cast(Literal["live", "demo"], mode),
        market=cast(Literal["future", "spot"], market),
        symbol=symbol,
        period=cast(VALID_PERIODS, period),
    )
