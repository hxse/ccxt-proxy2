from datetime import date, datetime
from typing import Any, Literal

import numpy as np
import pandas as pd

TqSerialKind = Literal["kline", "tick"]
KLINE_PRICE_COLUMNS = ("open", "high", "low", "close")
TICK_PRICE_COLUMNS = (
    "last_price",
    "average",
    "highest",
    "lowest",
    "bid_price1",
    "ask_price1",
)


class TqDataFrameError(ValueError):
    def __init__(self, detail: str = "TQ_INVALID_TIME_AXIS"):
        super().__init__(detail)
        self.detail = detail


def to_pandas_frame(frame: Any) -> pd.DataFrame:
    if isinstance(frame, pd.DataFrame):
        return frame.copy()
    return pd.DataFrame(frame).copy()


def clean_tq_serial_records(frame: Any, kind: TqSerialKind) -> list[dict[str, Any]]:
    target = normalize_tq_serial_frame(to_pandas_frame(frame), kind)
    return sanitize_json_frame(target).to_dict(orient="records")


def normalize_tq_serial_frame(frame: pd.DataFrame, kind: TqSerialKind) -> pd.DataFrame:
    price_columns = KLINE_PRICE_COLUMNS if kind == "kline" else TICK_PRICE_COLUMNS
    _ensure_columns(frame, ("datetime", *price_columns))
    if frame.empty:
        return frame.copy()

    datetime_values = pd.to_numeric(frame["datetime"], errors="coerce")
    prices = (
        frame.loc[:, list(price_columns)]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
    )
    placeholder = datetime_values.eq(0) & prices.isna().all(axis=1)
    data_positions = np.flatnonzero(~placeholder.to_numpy())
    if len(data_positions) == 0:
        return frame.iloc[0:0].copy()

    trimmed = frame.iloc[data_positions[0] :].copy()
    trimmed_datetime = pd.to_numeric(trimmed["datetime"], errors="coerce")
    if (
        trimmed_datetime.isna().any()
        or trimmed_datetime.le(0).any()
        or trimmed_datetime.diff().dropna().le(0).any()
    ):
        raise TqDataFrameError()
    trimmed_prices = (
        trimmed.loc[:, list(price_columns)]
        .apply(pd.to_numeric, errors="coerce")
        .replace([np.inf, -np.inf], np.nan)
    )
    if trimmed_prices.isna().all(axis=1).any():
        raise TqDataFrameError()
    trimmed["datetime"] = trimmed_datetime.astype("int64")
    return trimmed


def sanitize_json_frame(frame: pd.DataFrame) -> pd.DataFrame:
    target = frame.copy()
    numeric_columns = target.select_dtypes(include=["number"]).columns
    if len(numeric_columns):
        target.loc[:, numeric_columns] = target.loc[:, numeric_columns].replace(
            [np.inf, -np.inf], np.nan
        )
    return target.astype(object).where(pd.notna(target), None)


def history_wide_frame_to_records(frame: Any) -> list[dict[str, Any]]:
    target = to_pandas_frame(frame)
    if target.empty:
        return []
    _ensure_columns(target, ("date",))
    symbol_columns = [column for column in target.columns if column != "date"]
    if not symbol_columns:
        return []

    long_frame = target.melt(
        id_vars=["date"],
        value_vars=symbol_columns,
        var_name="symbol",
        value_name="underlying_symbol",
    )
    text = long_frame["underlying_symbol"].astype("string").str.strip()
    valid = text.notna() & text.ne("") & text.str.lower().ne("nan")
    long_frame = long_frame.loc[valid].copy()
    long_frame["underlying_symbol"] = text.loc[valid].astype(str)
    long_frame["date"] = long_frame["date"].map(_stringify_date)
    return sanitize_json_frame(long_frame).to_dict(orient="records")


def frame_rows(frame: Any) -> list[dict[str, Any]]:
    return sanitize_json_frame(to_pandas_frame(frame)).to_dict(orient="records")


def _ensure_columns(frame: pd.DataFrame, columns: tuple[str, ...]) -> None:
    if any(column not in frame.columns for column in columns):
        raise TqDataFrameError()


def _stringify_date(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
