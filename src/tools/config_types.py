from typing import TypedDict

from src.base_types import ExchangeName, MarketType, ModeType


class UserConfig(TypedDict):
    password: str


class ProxyConfig(TypedDict, total=False):
    http: str


class ApiCredential(TypedDict, total=False):
    api_key: str
    secret: str


class ExchangeConfig(TypedDict, total=False):
    enable_proxy: bool
    test: ApiCredential
    live: ApiCredential


class ExchangeWhitelistItemConfig(TypedDict):
    exchange: ExchangeName
    market: MarketType
    mode: ModeType


class AppConfig(TypedDict, total=False):
    SECRET: str
    users: dict[str, UserConfig]
    proxy: ProxyConfig
    binance: ExchangeConfig
    kraken: ExchangeConfig
    exchange_whitelist: list[ExchangeWhitelistItemConfig]
