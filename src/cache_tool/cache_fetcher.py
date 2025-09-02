import pandas as pd
from .cache_utils import (
    period_to_ms,
    format_timestamp,
    convert_ms_timestamp_to_utc_datetime,
)


def mock_fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: any = None
) -> pd.DataFrame:
    """
    模拟生成 OHLCV 数据的 Pandas DataFrame。
    列: time, open, high, low, close, volume, date
    """
    time_step_ms = period_to_ms(period)
    data = []
    for i in range(count):
        timestamp_ms = start_time + i * time_step_ms
        dt_object = convert_ms_timestamp_to_utc_datetime(timestamp_ms)
        formatted_date = format_timestamp(dt_object)
        data.append(
            [
                timestamp_ms,
                100 + i,
                105 + i,
                98 + i,
                102 + i,
                1000 + i,
                formatted_date,
            ]
        )
    return pd.DataFrame(
        data, columns=["time", "open", "high", "low", "close", "volume", "date"]
    )


def fetch_ohlcv(
    symbol: str, period: str, start_time: int, count: int, exchange: any = None
) -> pd.DataFrame:
    if not exchange:
        return pd.DataFrame()
    data = exchange.fetchOHLCV(symbol, period, start_time, count)
    return pd.DataFrame(
        data, columns=["time", "open", "high", "low", "close", "volume"]
    )
