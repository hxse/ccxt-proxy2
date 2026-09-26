"""后台计划与私有 HTTP 客户端配置；不作为用户路由默认参数。"""

import re
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Period = Literal["5m", "1h", "1d", "1w"]


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class MarketDataClientConfig(_StrictModel):
    base_url: str = "http://127.0.0.1:5123"
    user: str | None = None
    request_timeout_seconds: float = Field(60, gt=0, allow_inf_nan=False)

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, value):
        parts = urlsplit(value)
        if (
            parts.scheme not in {"http", "https"}
            or not parts.hostname
            or parts.username
            or parts.password
            or parts.query
            or parts.fragment
        ):
            raise ValueError(
                "base_url must be an absolute HTTP(S) address without credentials, query or fragment"
            )
        if parts.port is not None and not 1 <= parts.port <= 65535:
            raise ValueError("invalid base_url port")
        return value.rstrip("/")

    @field_validator("user")
    @classmethod
    def validate_user(cls, value):
        if value is not None and not value.strip():
            raise ValueError("user must be nonempty")
        return value


class PipelinePlan(_StrictModel):
    interval_seconds: int = Field(3600, gt=0)


class TqCollectionPlan(_StrictModel):
    enabled: bool = True
    timeframes: list[Period] = Field(
        default_factory=lambda: list[Period](["5m", "1h", "1d", "1w"]), min_length=1
    )
    data_length: int = Field(10000, ge=1, le=10000)
    save_main: bool = True
    save_weighted: bool = True
    save_mapping: bool = True
    mapping_timeframes: list[Period] = Field(
        default_factory=lambda: list[Period](["5m"])
    )
    save_transition: bool = True
    save_calendar: bool = True
    transition_timeframes: list[Period] = Field(
        default_factory=lambda: list[Period](["5m"])
    )
    calendar_lookback_years: int = Field(10, gt=0)
    symbols: dict[str, str] = Field(default_factory=dict)

    @field_validator("timeframes", "mapping_timeframes", "transition_timeframes")
    @classmethod
    def unique_periods(cls, values):
        if len(set(values)) != len(values):
            raise ValueError("duplicate periods")
        return values

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, values):
        if len(set(values.values())) != len(values) or any(
            not alias.strip()
            or not re.fullmatch(r"KQ\.m@[A-Z]+\.[A-Za-z0-9_]+", symbol)
            for alias, symbol in values.items()
        ):
            raise ValueError("invalid or duplicate collection symbols")
        return values

    @model_validator(mode="after")
    def validate_dependencies(self):
        if not self.enabled:
            return self
        if (self.save_main or self.save_weighted) and not self.symbols:
            raise ValueError("enabled symbol collection requires symbols")
        if self.save_mapping and (
            not self.save_main
            or not self.mapping_timeframes
            or not set(self.mapping_timeframes) <= set(self.timeframes)
        ):
            raise ValueError(
                "mapping requires main and a nonempty subset of timeframes"
            )
        if self.save_transition and (
            not self.save_mapping
            or not self.transition_timeframes
            or not set(self.transition_timeframes) <= set(self.mapping_timeframes)
        ):
            raise ValueError(
                "transition requires mapping and a nonempty subset of mapping_timeframes"
            )
        return self

    @property
    def has_work(self):
        return self.enabled and (
            self.save_main or self.save_weighted or self.save_calendar
        )


class RetentionModePlan(_StrictModel):
    live: int = Field(30000, ge=0)
    sandbox: int = Field(0, ge=0)


class AuxiliaryPlan(_StrictModel):
    keep_years: int = Field(10, gt=0)


class RetentionPlan(_StrictModel):
    enabled: bool = True
    providers: list[Literal["tq", "ccxt"]] = Field(
        default_factory=lambda: list[Literal["tq", "ccxt"]](["tq", "ccxt"]),
        min_length=1,
    )
    modes: RetentionModePlan = Field(default_factory=RetentionModePlan)
    auxiliary: AuxiliaryPlan = Field(default_factory=AuxiliaryPlan)

    @field_validator("providers")
    @classmethod
    def validate_providers(cls, values):
        if len(set(values)) != len(values):
            raise ValueError("duplicate retention providers")
        return values


class MarketDataPlan(_StrictModel):
    pipeline: PipelinePlan = Field(default_factory=PipelinePlan)
    tq_collection: TqCollectionPlan = Field(default_factory=TqCollectionPlan)
    retention: RetentionPlan = Field(default_factory=RetentionPlan)
