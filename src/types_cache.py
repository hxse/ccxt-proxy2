from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

from src.base_types import NonEmptyString
from src.cache_tool.maintenance_models import RetentionPolicy


class CacheSummaryQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: Literal["binance", "kraken", "tq"] | None = Field(
        None, description="实际数据源，精确匹配"
    )
    mode: Literal["live", "sandbox"] | None = Field(
        None, description="缓存环境；TQ 为 live"
    )
    market: NonEmptyString | None = Field(None, description="行情市场身份，精确匹配")
    symbol: NonEmptyString | None = Field(
        None, description="完整品种或合约代码，精确匹配"
    )
    timeframe: NonEmptyString | None = Field(
        None, description="存储周期身份；TQ 例如 300s"
    )
    variant: NonEmptyString | None = Field(
        None, description="价格或复权身份，默认价格为 default"
    )
    kind: Literal["ohlcv", "calendar", "main_mapping", "transition"] | None = Field(
        None, description="缓存数据类型"
    )


class RetentionModes(BaseModel):
    model_config = ConfigDict(extra="forbid")
    live: StrictInt = Field(ge=0)
    sandbox: StrictInt = Field(ge=0)


class AuxiliaryRetention(BaseModel):
    model_config = ConfigDict(extra="forbid")
    keep_years: StrictInt = Field(gt=0)


class CachePruneRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    providers: list[Literal["tq", "ccxt"]] = Field(min_length=1)
    modes: RetentionModes
    auxiliary: AuxiliaryRetention | None = None

    @model_validator(mode="after")
    def validate_policy(self):
        if len(set(self.providers)) != len(self.providers):
            raise ValueError("providers must not contain duplicates")
        if self.auxiliary is not None and "tq" not in self.providers:
            raise ValueError("auxiliary retention requires tq")
        return self

    def policy(self) -> RetentionPolicy:
        return RetentionPolicy(
            tuple(self.providers),
            self.modes.live,
            self.modes.sandbox,
            self.auxiliary.keep_years if self.auxiliary else None,
        )
