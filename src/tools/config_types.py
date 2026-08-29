import re

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.base_types import ExchangeName, MarketType, ModeType

CHAT_ALIAS_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


class UserConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    password: str = Field(..., min_length=1)


class ProxyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    http: str | None = None
    https: str | None = None
    legacy_http: str | None = Field(default=None, alias="_http")
    legacy_https: str | None = Field(default=None, alias="_https")
    alt_http: str | None = Field(default=None, alias="__http")
    alt_https: str | None = Field(default=None, alias="__https")

    @property
    def effective_http(self) -> str | None:
        return (
            self.http
            or self.https
            or self.legacy_http
            or self.legacy_https
            or self.alt_http
            or self.alt_https
        )


class ApiCredential(BaseModel):
    model_config = ConfigDict(extra="forbid")

    api_key: str = Field(..., min_length=1)
    secret: str = Field(..., min_length=1)


class ExchangeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enable_proxy: bool = False
    test: ApiCredential | None = None
    live: ApiCredential | None = None


class TqConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    username: str | None = None
    password: str = ""


class OhlcvCacheConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database_path: str = "./data/cache/ohlcv.duckdb"
    max_rows_per_series: int = Field(2_000_000, gt=100_000)
    max_rows_total: int = Field(20_000_000, gt=100_000)

    @model_validator(mode="after")
    def validate_limits(self) -> "OhlcvCacheConfig":
        if self.max_rows_per_series > self.max_rows_total:
            raise ValueError("ohlcv cache per-series limit must not exceed total limit")
        return self


class TelegramConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bot_token: str = Field(..., min_length=1)
    chats: dict[str, str] = Field(..., min_length=1)

    @field_validator("bot_token")
    @classmethod
    def validate_bot_token(cls, bot_token: str) -> str:
        normalized = bot_token.strip()
        if not normalized:
            raise ValueError("telegram.bot_token must not be empty")
        return normalized

    @field_validator("chats")
    @classmethod
    def validate_chats(cls, chats: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for alias, chat_id in chats.items():
            if not alias or not CHAT_ALIAS_PATTERN.fullmatch(alias):
                raise ValueError("telegram chat aliases must match [A-Za-z0-9_-]+")
            normalized_chat_id = chat_id.strip()
            if not normalized_chat_id:
                raise ValueError("telegram chat ids must not be empty")
            normalized[alias] = normalized_chat_id
        return normalized


class ExchangeWhitelistItemConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange: ExchangeName
    market: MarketType
    mode: ModeType


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    SECRET: str = Field(..., min_length=1)
    users: dict[str, UserConfig] = Field(default_factory=dict)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    binance: ExchangeConfig | None = None
    kraken: ExchangeConfig | None = None
    tq: TqConfig | None = None
    ohlcv_cache: OhlcvCacheConfig = Field(default_factory=OhlcvCacheConfig)
    telegram: TelegramConfig | None = None
    exchange_whitelist: list[ExchangeWhitelistItemConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_exchange_whitelist_dependencies(self) -> "AppConfig":
        for item in self.exchange_whitelist:
            if (
                item.exchange == "kraken"
                and item.market == "spot"
                and item.mode == "sandbox"
            ):
                raise ValueError("kraken spot sandbox is not supported")
            exchange_config = getattr(self, item.exchange)
            if exchange_config is None:
                raise ValueError(
                    f"missing config for exchange '{item.exchange}' referenced by exchange_whitelist"
                )

            credentials = (
                exchange_config.test if item.mode == "sandbox" else exchange_config.live
            )
            if credentials is None:
                raise ValueError(
                    f"missing {item.mode} credentials for exchange '{item.exchange}'"
                )

            if exchange_config.enable_proxy and not self.proxy.effective_http:
                raise ValueError(
                    f"proxy.http must be configured when {item.exchange}.enable_proxy is true"
                )

        return self
