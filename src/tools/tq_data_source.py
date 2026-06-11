from datetime import date, datetime
from typing import Any, Literal

import polars as pl


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


def to_polars_frame(frame: Any) -> pl.DataFrame:
    if isinstance(frame, pl.DataFrame):
        return frame.clone()
    return pl.from_pandas(frame)


def clean_tq_serial_records(frame: Any, kind: TqSerialKind) -> list[dict[str, Any]]:
    target = normalize_tq_serial_frame(to_polars_frame(frame), kind)
    target = sanitize_json_frame(target)
    return target.to_dicts()


def normalize_tq_serial_frame(frame: pl.DataFrame, kind: TqSerialKind) -> pl.DataFrame:
    price_columns = _price_columns(kind)
    _ensure_columns(frame, ("datetime", *price_columns))

    if frame.is_empty():
        return frame

    placeholder = _placeholder_expr(price_columns)
    with_flags = frame.with_columns(placeholder.alias("__placeholder")).with_row_index(
        "__row"
    )
    first_data_row = with_flags.filter(~pl.col("__placeholder")).select(
        pl.col("__row").min()
    ).item()

    if first_data_row is None:
        return frame.head(0)

    trimmed = with_flags.filter(pl.col("__row") >= first_data_row).drop(
        ["__row", "__placeholder"]
    )
    trimmed = trimmed.with_columns(
        pl.col("datetime").cast(pl.Int64, strict=False).alias("datetime")
    )
    _validate_time_axis(trimmed)
    _validate_price_columns(trimmed, price_columns)
    return trimmed


def sanitize_json_frame(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column, dtype in frame.schema.items():
        if dtype in (pl.Float32, pl.Float64):
            expressions.append(
                pl.when(pl.col(column).is_finite())
                .then(pl.col(column))
                .otherwise(None)
                .alias(column)
            )
    if not expressions:
        return frame
    return frame.with_columns(expressions)


def history_wide_frame_to_records(frame: Any) -> list[dict[str, Any]]:
    target = to_polars_frame(frame)
    if target.is_empty():
        return []
    _ensure_columns(target, ("date",))

    symbol_columns = [column for column in target.columns if column != "date"]
    if not symbol_columns:
        return []

    long_frame = target.unpivot(
        index="date",
        on=symbol_columns,
        variable_name="symbol",
        value_name="underlying_symbol",
    )
    long_frame = long_frame.with_columns(
        [
            _date_to_string_expr("date"),
            _clean_text_expr("underlying_symbol"),
        ]
    ).filter(
        pl.col("underlying_symbol").is_not_null()
    )
    return long_frame.to_dicts()


def _price_columns(kind: TqSerialKind) -> tuple[str, ...]:
    if kind == "kline":
        return KLINE_PRICE_COLUMNS
    return TICK_PRICE_COLUMNS


def _ensure_columns(frame: pl.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise TqDataFrameError()


def _empty_number_expr(column: str) -> pl.Expr:
    value = pl.col(column).cast(pl.Float64, strict=False)
    return value.is_null() | value.is_nan()


def _null_or_nan_expr(column: str) -> pl.Expr:
    value = pl.col(column)
    numeric = value.cast(pl.Float64, strict=False)
    return value.is_null() | numeric.is_nan().fill_null(False)


def _clean_text_expr(column: str) -> pl.Expr:
    text = pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars()
    invalid = (
        _null_or_nan_expr(column)
        | text.is_null()
        | (text == "")
        | (text.str.to_lowercase() == "nan")
    )
    return pl.when(invalid).then(None).otherwise(text).alias(column)


def _placeholder_expr(price_columns: tuple[str, ...]) -> pl.Expr:
    return (pl.col("datetime").cast(pl.Int64, strict=False) == 0) & pl.all_horizontal(
        *[_empty_number_expr(column) for column in price_columns]
    )


def _validate_time_axis(frame: pl.DataFrame) -> None:
    if frame.is_empty():
        return

    datetime_expr = pl.col("datetime").cast(pl.Int64, strict=False)
    invalid_datetime = frame.select(
        (
            datetime_expr.is_null()
            | (datetime_expr <= 0)
            | (datetime_expr.diff().fill_null(1) <= 0)
        )
        .any()
        .alias("invalid")
    ).item()
    if bool(invalid_datetime):
        raise TqDataFrameError()


def _validate_price_columns(
    frame: pl.DataFrame, price_columns: tuple[str, ...]
) -> None:
    if frame.is_empty():
        return

    price_empty = frame.select(
        pl.all_horizontal(*[_empty_number_expr(column) for column in price_columns])
        .any()
        .alias("invalid")
    ).item()
    if bool(price_empty):
        raise TqDataFrameError()


def _date_to_string_expr(column: str) -> pl.Expr:
    value = pl.col(column)
    return (
        pl.when(value.cast(pl.Utf8, strict=False).is_null())
        .then(None)
        .otherwise(value.map_elements(_stringify_date, return_dtype=pl.Utf8))
        .alias(column)
    )


def _stringify_date(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def frame_rows(frame: Any) -> list[dict[str, Any]]:
    return sanitize_json_frame(to_polars_frame(frame)).to_dicts()
