from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.base_types import ExchangeName, MarketType, ModeType


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
    exchange_whitelist: list[ExchangeWhitelistItemConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_exchange_whitelist_dependencies(self) -> "AppConfig":
        for item in self.exchange_whitelist:
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
