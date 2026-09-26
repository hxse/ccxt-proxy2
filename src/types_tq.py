from datetime import date
from typing import Annotated

from fastapi import Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.router.query_validation import reject_unknown_query_params
from src.tq_validation import (
    DEFAULT_TQ_DATA_LENGTH,
    TqAdjType,
    _http_validation_error,
    _normalize_duration_seconds,
    _normalize_symbol,
    _validate_adj_type,
    _validate_calendar_range,
    _validate_data_length,
    _validate_duration_seconds,
    _validate_symbol,
    _validate_symbols,
    transition_duration,
)
from src.tq_validation import (
    MAX_TQ_DATA_LENGTH as MAX_TQ_DATA_LENGTH,
)
from src.tq_validation import (
    MAX_TQ_OHLCV_LENGTH as MAX_TQ_OHLCV_LENGTH,
)
from src.tq_validation import (
    TQ_ADJ_TYPE_QUERY_ENUM as TQ_ADJ_TYPE_QUERY_ENUM,
)


class TqOhlcvRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(..., title="TQ symbol")
    duration_seconds: int = Field(..., gt=0, title="K线周期，单位秒")
    data_length: int = Field(
        DEFAULT_TQ_DATA_LENGTH,
        ge=1,
        le=MAX_TQ_OHLCV_LENGTH,
        title="最多响应 K 线数量",
    )
    adj_type: TqAdjType | None = Field(None, title="TQ 复权参数")

    enable_cache: bool = Field(True, title="启用项目磁盘缓存")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str) -> str:
        return _normalize_symbol(symbol)

    @field_validator("duration_seconds")
    @classmethod
    def validate_duration_seconds(cls, duration_seconds: int) -> int:
        return _normalize_duration_seconds(duration_seconds)


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

    symbol: str = Field(..., title="单个 TQ 主连 symbol")
    start_time: int | None = Field(
        None,
        strict=True,
        gt=0,
        le=2**63 - 1,
        description="包含式历史起点，整数 Unix 纳秒",
    )
    end_time: int | None = Field(
        None,
        strict=True,
        gt=0,
        le=2**63 - 1,
        description="包含式历史终点，整数 Unix 纳秒",
    )
    enable_cache: bool = True
    transition_timeframe: str | None = None
    transition_bars: int = Field(10, strict=True, ge=1, le=MAX_TQ_DATA_LENGTH - 1)

    @field_validator("transition_timeframe")
    @classmethod
    def validate_transition_timeframe(cls, value: str | None):
        if value is not None:
            transition_duration(value)
        return value

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str) -> str:
        return _normalize_symbol(symbol)

    @model_validator(mode="after")
    def validate_range(self):
        if (self.start_time is None) != (self.end_time is None):
            raise ValueError("TQ_INVALID_DATE_RANGE")
        if (
            self.start_time is not None
            and self.end_time is not None
            and self.start_time > self.end_time
        ):
            raise ValueError("TQ_INVALID_DATE_RANGE")
        if self.transition_timeframe is not None and self.start_time is None:
            raise ValueError("TQ_INVALID_DATE_RANGE")
        return self


class TqTradingCalendarRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_date: date = Field(
        description=(
            "起始中国期货日历日（Asia/Shanghai），包含当天；"
            "仅为 YYYY-MM-DD date，不是 UTC 时间戳，不做时区换算。"
        )
    )
    end_date: date = Field(
        description=(
            "结束中国期货日历日（Asia/Shanghai），包含当天；"
            "仅为 YYYY-MM-DD date，不是 UTC 时间戳，不做时区换算。"
        )
    )

    enable_cache: bool = Field(True, description="启用日历缓存；仍先在线检查官方覆盖")

    @model_validator(mode="after")
    def validate_range(self) -> "TqTradingCalendarRequest":
        if self.start_date > self.end_date:
            raise ValueError("TQ_INVALID_DATE_RANGE")
        return self


class TqTradingStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    symbol: str = Field(
        min_length=1,
        description="单个完整 TQ 合约代码，如 SHFE.rb2610。按原样交给 get_trading_status，不拆分交易所/品种，不自动解析主连。",
        examples=["SHFE.rb2610"],
    )

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, symbol: str) -> str:
        return _normalize_symbol(symbol)


def tq_ohlcv_request(
    request: Request,
    symbol: Annotated[
        list[str],
        Query(
            title="TQ symbol",
            description=(
                "完整单个 TQ symbol；重复 symbol 参数拒绝。普通合约示例 SHFE.rb2505，主连示例 "
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
                "最多返回数量，默认 10000，范围 1..100000；SDK 单次取 min(N,10000)，不足时复用相连缓存；"
                "有效历史不足或前置占位行被裁剪时，响应数量允许少于该值。"
            ),
            examples=[10000],
            json_schema_extra={"minimum": 1, "maximum": MAX_TQ_OHLCV_LENGTH},
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
    enable_cache: Annotated[
        bool, Query(description="同时控制项目缓存读写与休市兜底")
    ] = True,
) -> TqOhlcvRequest:
    reject_unknown_query_params(
        request,
        {"symbol", "duration_seconds", "data_length", "adj_type", "enable_cache"},
    )
    symbols = _validate_symbols(symbol)
    if len(symbols) != 1:
        raise _http_validation_error("TQ_MULTIPLE_SYMBOLS_NOT_SUPPORTED")
    return TqOhlcvRequest(
        symbol=symbols[0],
        duration_seconds=_validate_duration_seconds(duration_seconds),
        data_length=_validate_data_length(data_length, MAX_TQ_OHLCV_LENGTH),
        adj_type=_validate_adj_type(adj_type),
        enable_cache=enable_cache,
    )


def tq_tick_request(
    request: Request,
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
    reject_unknown_query_params(request, {"symbol", "data_length", "adj_type"})
    return TqTickRequest(
        symbol=_validate_symbol(symbol),
        data_length=_validate_data_length(data_length),
        adj_type=_validate_adj_type(adj_type),
    )


def tq_underlying_symbol_request(
    request: Request,
    symbol: Annotated[
        list[str],
        Query(
            description="一个完整 CONT 主连代码；重复 symbol 拒绝",
            examples=["KQ.m@SHFE.rb"],
        ),
    ],
    start_time: Annotated[
        int | None,
        Query(description="包含式起点，正整数 Unix 纳秒", gt=0, le=2**63 - 1),
    ] = None,
    end_time: Annotated[
        int | None,
        Query(description="包含式终点，正整数 Unix 纳秒", gt=0, le=2**63 - 1),
    ] = None,
    enable_cache: Annotated[
        bool, Query(description="项目缓存读写；自动核验仍执行")
    ] = True,
    transition_timeframe: Annotated[
        str | None, Query(description="可选旧价格周期，s/m/h/d/w；不支持 M")
    ] = None,
    transition_bars: Annotated[
        int,
        Query(
            description="过渡数量，默认十根",
            json_schema_extra={"minimum": 1, "maximum": MAX_TQ_DATA_LENGTH - 1},
        ),
    ] = 10,
) -> TqUnderlyingSymbolRequest:
    reject_unknown_query_params(
        request,
        {
            "symbol",
            "start_time",
            "end_time",
            "enable_cache",
            "transition_timeframe",
            "transition_bars",
        },
    )
    if not 1 <= transition_bars < MAX_TQ_DATA_LENGTH:
        raise _http_validation_error("TQ_INVALID_TRANSITION_BARS")
    if transition_timeframe is not None:
        try:
            transition_duration(transition_timeframe)
        except ValueError as exc:
            raise _http_validation_error(str(exc)) from exc
        if start_time is None:
            raise _http_validation_error("TQ_INVALID_DATE_RANGE")
    symbols = _validate_symbols(symbol)
    if len(symbols) != 1:
        raise _http_validation_error("TQ_MULTIPLE_SYMBOLS_NOT_SUPPORTED")
    if (start_time is None) != (end_time is None) or (
        start_time is not None and end_time is not None and start_time > end_time
    ):
        raise _http_validation_error("TQ_INVALID_DATE_RANGE")
    return TqUnderlyingSymbolRequest(
        symbol=symbols[0],
        start_time=start_time,
        end_time=end_time,
        enable_cache=enable_cache,
        transition_timeframe=transition_timeframe,
        transition_bars=transition_bars,
    )


def tq_trading_calendar_request(
    request: Request,
    start_date: Annotated[
        date,
        Query(
            title="起始日期",
            description=(
                "中国期货日历的起始自然日（Asia/Shanghai），"
                "ISO 8601 YYYY-MM-DD，结果包含该日。"
                "该值不是 UTC 毫秒时间戳，不做时区换算。"
            ),
            examples=["2026-09-01"],
        ),
    ],
    end_date: Annotated[
        date,
        Query(
            title="结束日期",
            description=(
                "中国期货日历的结束自然日（Asia/Shanghai），"
                "ISO 8601 YYYY-MM-DD，结果包含该日。"
                "该值不是 UTC 毫秒时间戳，不做时区换算。"
            ),
            examples=["2026-09-30"],
        ),
    ],
    enable_cache: Annotated[
        bool, Query(description="启用项目缓存；仍自动核验官方覆盖")
    ] = True,
) -> TqTradingCalendarRequest:
    reject_unknown_query_params(request, {"start_date", "end_date", "enable_cache"})
    _validate_calendar_range(start_date, end_date)
    return TqTradingCalendarRequest(
        start_date=start_date, end_date=end_date, enable_cache=enable_cache
    )
