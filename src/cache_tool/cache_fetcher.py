import polars as pl
from typing import Any
from .cache_utils import (
    period_to_ms,
    format_timestamp,
    convert_ms_timestamp_to_utc_datetime,
    time_format,
)


def mock_fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: Any = None
) -> pl.DataFrame:
    """
    模拟生成 OHLCV 数据的 Pandas DataFrame。
    列: time, open, high, low, close, volume, date
    """
    time_step_ms = period_to_ms(period)
    data = []
    for i in range(count):
        timestamp_ms = start_time + i * time_step_ms
        data.append(
            [
                timestamp_ms,
                100 + i,
                105 + i,
                98 + i,
                102 + i,
                1000 + i,
                # formatted_date,
            ]
        )

    # 1. 创建 schema，只包含 data 中已有的列
    schema = {
        "time": pl.Int64,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
    }

    df = pl.DataFrame(data, schema=schema, orient="row")

    # 2. 添加新的 date 列
    return df.with_columns(
        pl.col("time")
        .cast(pl.Datetime(time_unit="ms", time_zone="UTC"))
        .dt.strftime(time_format)
        .alias("date")
    )


def fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: Any = None
) -> pl.DataFrame:
    if not exchange:
        return pl.DataFrame()
    data = exchange.fetchOHLCV(symbol, period, start_time, count)

    # 1. 创建 schema，只包含 data 中已有的列
    schema = {
        "time": pl.Int64,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Int64,
    }

    df = pl.DataFrame(data, schema=schema, orient="row")

    # 2. 添加新的 date 列
    return df.with_columns(
        pl.col("time")
        .cast(pl.Datetime(time_unit="ms", time_zone="UTC"))
        .dt.strftime(time_format)
        .alias("date")
    )
