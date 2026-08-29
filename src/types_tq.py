from typing import Annotated, Literal

from fastapi import HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator

TqAdjType = Literal["F", "B", "FORWARD", "BACK"]

DEFAULT_TQ_DATA_LENGTH = 10000
MAX_TQ_DATA_LENGTH = 10000
TQ_ADJ_TYPE_QUERY_ENUM = ["", "F", "B", "FORWARD", "BACK"]


class TqOhlcvRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str | list[str] = Field(..., title="TQ symbol")
    duration_seconds: int = Field(..., gt=0, title="K线周期，单位秒")
    data_length: int = Field(
        DEFAULT_TQ_DATA_LENGTH,
        ge=1,
        le=MAX_TQ_DATA_LENGTH,
        title="TQ serial 窗口宽度",
    )
    adj_type: TqAdjType | None = Field(None, title="TQ 复权参数")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str | list[str]) -> str | list[str]:
        return _normalize_symbol_input(symbol)

    @field_validator("duration_seconds")
    @classmethod
    def validate_duration_seconds(cls, duration_seconds: int) -> int:
        return _normalize_duration_seconds(duration_seconds)

    @property
    def symbol_list(self) -> list[str]:
        return self.symbol if isinstance(self.symbol, list) else [self.symbol]


class TqTickRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(..., title="TQ symbol")
    data_length: int = Field(
        DEFAULT_TQ_DATA_LENGTH,
        ge=1,
        le=MAX_TQ_DATA_LENGTH,
        title="TQ serial 窗口宽度",
    )
    adj_type: TqAdjType | None = Field(None, title="TQ 复权参数")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str) -> str:
        return _normalize_symbol(symbol)


class TqUnderlyingSymbolRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str | list[str] = Field(..., title="TQ 主连 symbol")
    n: int | None = Field(None, gt=0, title="最近 N 个交易日的主连映射")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str | list[str]) -> str | list[str]:
        return _normalize_symbol_input(symbol)

    @property
    def symbol_list(self) -> list[str]:
        return self.symbol if isinstance(self.symbol, list) else [self.symbol]


def _normalize_symbol_input(symbol: str | list[str]) -> str | list[str]:
    if isinstance(symbol, list):
        return _normalize_symbols(symbol)
    return _normalize_symbol(symbol)


def _normalize_symbols(symbols: list[str]) -> list[str]:
    normalized = [symbol.strip() for symbol in symbols]
    if not normalized or any(not symbol for symbol in normalized):
        raise ValueError("TQ_INVALID_SYMBOL")
    return normalized


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip()
    if not normalized:
        raise ValueError("TQ_INVALID_SYMBOL")
    return normalized


def _normalize_duration_seconds(duration_seconds: int) -> int:
    if duration_seconds <= 0:
        raise ValueError("TQ_INVALID_DURATION_SECONDS")
    if duration_seconds > 86400 and duration_seconds % 86400 != 0:
        raise ValueError("TQ_INVALID_DURATION_SECONDS")
    return duration_seconds


def _http_validation_error(code: str) -> HTTPException:
    return HTTPException(status_code=400, detail=code)


def _validate_symbols(symbols: list[str]) -> list[str]:
    try:
        return _normalize_symbols(symbols)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_symbol(symbol: str) -> str:
    try:
        return _normalize_symbol(symbol)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_duration_seconds(duration_seconds: int) -> int:
    try:
        return _normalize_duration_seconds(duration_seconds)
    except ValueError as exc:
        raise _http_validation_error(str(exc)) from exc


def _validate_data_length(data_length: int) -> int:
    if data_length < 1 or data_length > MAX_TQ_DATA_LENGTH:
        raise _http_validation_error("TQ_INVALID_DATA_LENGTH")
    return data_length


def _validate_adj_type(adj_type: str | None) -> TqAdjType | None:
    if adj_type == "":
        return None
    if adj_type is None:
        return None
    if adj_type == "F":
        return "F"
    if adj_type == "B":
        return "B"
    if adj_type == "FORWARD":
        return "FORWARD"
    if adj_type == "BACK":
        return "BACK"
    raise HTTPException(status_code=400, detail="TQ_INVALID_ADJ_TYPE")


def _validate_n(n: int | None) -> int | None:
    if n is not None and n <= 0:
        raise _http_validation_error("TQ_INVALID_DATA_LENGTH")
    return n


def tq_ohlcv_request(
    symbol: Annotated[
        list[str],
        Query(
            title="TQ symbol",
            description=(
                "完整 TQ symbol。单合约传一个 symbol；多合约 K 线用重复 query "
                "参数传多个同名 symbol。普通合约示例 SHFE.rb2505，主连示例 "
                "KQ.m@SHFE.rb，指数/加权示例 KQ.i@SHFE.rb。"
            ),
            examples=["SHFE.rb2505", "KQ.m@SHFE.rb", "KQ.i@SHFE.rb"],
        ),
    ],
    duration_seconds: Annotated[
        int,
        Query(
            title="K线周期，单位秒",
            description=(
                "透传 TQ duration_seconds。必须大于 0；超过 86400 秒时必须是 "
                "86400 的整数倍，否则返回 400 TQ_INVALID_DURATION_SECONDS。"
            ),
            examples=[60],
            json_schema_extra={"exclusiveMinimum": 0},
        ),
    ],
    data_length: Annotated[
        int,
        Query(
            title="TQ serial 窗口宽度",
            description=(
                "透传 TQ data_length，默认 10000，范围 1..10000。它是请求 "
                "TQ 实时序列的窗口宽度上限，不保证响应至少返回这么多行；"
                "有效历史不足或前置占位行被裁剪时，响应数量允许少于该值。"
            ),
            examples=[10000],
            json_schema_extra={"minimum": 1, "maximum": MAX_TQ_DATA_LENGTH},
        ),
    ] = DEFAULT_TQ_DATA_LENGTH,
    adj_type: Annotated[
        str | None,
        Query(
            title="TQ 复权参数",
            description=(
                "透传 TQ adj_type。允许 F、B、FORWARD、BACK 或空；空字符串会按 "
                "None 处理。非法值返回 400 TQ_INVALID_ADJ_TYPE。"
            ),
            examples=["F"],
            json_schema_extra={"enum": TQ_ADJ_TYPE_QUERY_ENUM},
        ),
    ] = None,
) -> TqOhlcvRequest:
    symbols = _validate_symbols(symbol)
    request_symbol: str | list[str] = symbols[0] if len(symbols) == 1 else symbols
    return TqOhlcvRequest(
        symbol=request_symbol,
        duration_seconds=_validate_duration_seconds(duration_seconds),
        data_length=_validate_data_length(data_length),
        adj_type=_validate_adj_type(adj_type),
    )


def tq_tick_request(
    symbol: Annotated[
        str,
        Query(
            title="TQ symbol",
            description=(
                "完整 TQ symbol。Tick serial 只接受单个 symbol，例如 "
                "SHFE.rb2505 或 KQ.m@SHFE.rb。"
            ),
            examples=["SHFE.rb2505"],
        ),
    ],
    data_length: Annotated[
        int,
        Query(
            title="TQ serial 窗口宽度",
            description=(
                "透传 TQ data_length，默认 10000，范围 1..10000。它是请求 "
                "TQ Tick 实时序列的窗口宽度上限，不保证响应至少返回这么多行。"
            ),
            examples=[10000],
            json_schema_extra={"minimum": 1, "maximum": MAX_TQ_DATA_LENGTH},
        ),
    ] = DEFAULT_TQ_DATA_LENGTH,
    adj_type: Annotated[
        str | None,
        Query(
            title="TQ 复权参数",
            description=(
                "透传 TQ adj_type。允许 F、B、FORWARD、BACK 或空；空字符串会按 "
                "None 处理。非法值返回 400 TQ_INVALID_ADJ_TYPE。"
            ),
            examples=["F"],
            json_schema_extra={"enum": TQ_ADJ_TYPE_QUERY_ENUM},
        ),
    ] = None,
) -> TqTickRequest:
    return TqTickRequest(
        symbol=_validate_symbol(symbol),
        data_length=_validate_data_length(data_length),
        adj_type=_validate_adj_type(adj_type),
    )


def tq_underlying_symbol_request(
    symbol: Annotated[
        list[str],
        Query(
            title="TQ 主连 symbol",
            description=(
                "TQ 主连 symbol。单个主连传一个 symbol；多个主连用重复 query "
                "参数传多个同名 symbol。典型格式为 KQ.m@DCE.i。"
            ),
            examples=["KQ.m@DCE.i"],
        ),
    ],
    n: Annotated[
        int | None,
        Query(
            title="最近 N 个交易日的主连映射",
            description=(
                "可选。传入后额外调用 TQ query_his_cont_quotes(symbol, n=n)，"
                "返回最近 N 个交易日的主连标的映射；不传时只返回当前标的。"
            ),
            examples=[20],
            json_schema_extra={"exclusiveMinimum": 0},
        ),
    ] = None,
) -> TqUnderlyingSymbolRequest:
    symbols = _validate_symbols(symbol)
    request_symbol: str | list[str] = symbols[0] if len(symbols) == 1 else symbols
    return TqUnderlyingSymbolRequest(
        symbol=request_symbol,
        n=_validate_n(n),
    )
