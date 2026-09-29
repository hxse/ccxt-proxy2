import re
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StrictBool,
    field_validator,
    model_validator,
)

from src.base_types import ExchangeName, MarketType, ModeType
from src.cfb.config import CfbConfig
from src.tools.deployment_types import DeploymentConfig
from src.tools.market_data_types import MarketDataClientConfig

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

    enable_proxy: bool = False
    username: str | None = None
    password: str = ""


class CtpAccountConfig(BaseModel):
    """一个 CTP 前置/账户；test 可使用 SimNow，live 使用期货公司实盘信息。"""

    model_config = ConfigDict(extra="forbid")

    trader_front: str = Field(pattern=r"^tcp://[^\s/:]+:[0-9]{1,5}$")
    broker_id: str = Field(min_length=1, max_length=10, pattern=r"^[!-~]+$")
    investor_id: str = Field(min_length=1, max_length=12, pattern=r"^[!-~]+$")
    user_id: str | None = Field(None, min_length=1, max_length=15, pattern=r"^[!-~]+$")
    password: SecretStr = Field(min_length=1, max_length=40)
    app_id: str | None = Field(None, min_length=1, max_length=32, pattern=r"^[!-~]+$")
    auth_code: SecretStr | None = Field(None, min_length=1, max_length=16)
    # SDK 采集库使用的生产/评测密钥模式，与模拟盘/实盘选择是不同的配置。
    production_mode: bool = True

    @model_validator(mode="after")
    def validate_ctp_fields(self) -> "CtpAccountConfig":
        if (self.app_id is None) != (self.auth_code is None):
            raise ValueError("ctp app_id and auth_code must be configured together")
        if not 1 <= int(self.trader_front.rsplit(":", 1)[1]) <= 65535:
            raise ValueError("ctp trader_front port must be in 1..65535")
        for value, capacity in ((self.password, 40), (self.auth_code, 16)):
            if value is not None:
                raw = value.get_secret_value()
                if "\x00" in raw or len(raw.encode("utf-8")) > capacity:
                    raise ValueError(
                        "ctp credential exceeds native field capacity or contains NUL"
                    )
        return self


class CtpConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    test: CtpAccountConfig | None = None
    live: CtpAccountConfig | None = None
    flow_path: str = Field("./data/ctp", min_length=1)
    connect_timeout_seconds: float = Field(10, gt=0, le=60, allow_inf_nan=False)
    request_timeout_seconds: float = Field(10, gt=0, le=60, allow_inf_nan=False)
    query_interval_seconds: float = Field(1.1, ge=1, le=60, allow_inf_nan=False)


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


class TradingServiceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    is_live: StrictBool

    @property
    def mode(self) -> ModeType:
        # 只在配置边界转换；SDK、缓存和既有持久化身份继续使用原环境名称。
        return "live" if self.is_live else "sandbox"


class CcxtServiceConfig(TradingServiceConfig):
    service: Literal["ccxt"]
    exchange: ExchangeName
    market: MarketType

    @property
    def identity(self) -> str:
        return f"ccxt/{self.exchange}/{self.market}/{self.mode}"


class TqServiceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service: Literal["tq"]

    @property
    def identity(self) -> str:
        return "tq"


class CtpServiceConfig(TradingServiceConfig):
    service: Literal["ctp"]

    @property
    def identity(self) -> str:
        return f"ctp/{self.mode}"


class CfbServiceConfig(TradingServiceConfig):
    service: Literal["cfb"]

    @property
    def identity(self) -> str:
        return f"cfb/{self.mode}"


ServiceWhitelistItem = Annotated[
    CcxtServiceConfig | TqServiceConfig | CtpServiceConfig | CfbServiceConfig,
    Field(discriminator="service"),
]


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    SECRET: str = Field(..., min_length=1)
    users: dict[str, UserConfig] = Field(default_factory=dict)
    proxy: ProxyConfig = Field(default_factory=ProxyConfig)
    binance: ExchangeConfig | None = None
    kraken: ExchangeConfig | None = None
    tq: TqConfig | None = None
    ctp: CtpConfig | None = None
    cfb: CfbConfig | None = None
    ohlcv_cache: OhlcvCacheConfig = Field(default_factory=OhlcvCacheConfig)
    market_data_client: MarketDataClientConfig = Field(
        default_factory=MarketDataClientConfig
    )
    telegram: TelegramConfig | None = None
    deployment: DeploymentConfig | None = None
    service_whitelist: list[ServiceWhitelistItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_service_whitelist_dependencies(self) -> "AppConfig":
        # 公共取时也使用币安代理，与交易身份是否启用无关。
        if (
            self.binance is not None
            and self.binance.enable_proxy
            and not self.proxy.effective_http
        ):
            raise ValueError(
                "proxy.http must be configured when binance.enable_proxy is true"
            )
        seen_identities: set[str] = set()
        cfb_ports: set[int] = set()
        for item in self.service_whitelist:
            identity = item.identity
            if identity in seen_identities:
                raise ValueError("duplicate service_whitelist identity: " + identity)
            seen_identities.add(identity)
            if item.service == "cfb":
                if self.cfb is None:
                    raise ValueError(
                        "missing cfb config referenced by service_whitelist"
                    )
                settings = self.cfb.for_mode(item.mode)
                if settings.vnc_enabled:
                    ports = {settings.vnc.port, settings.vnc.web_port}
                    if cfb_ports & ports:
                        raise ValueError("enabled CFB modes must use distinct VNC/noVNC ports")
                    cfb_ports.update(ports)
                if str(self.cfb.bridge.data_dir) != "/data":
                    raise ValueError("cfb.bridge.data_dir must be /data in the execution container")
                if self.cfb.enable_proxy:
                    from src.cfb.network import parse_proxy

                    parse_proxy(self.proxy.effective_http)
                continue
            if item.service == "tq":
                if self.tq is None:
                    raise ValueError(
                        "missing tq config referenced by service_whitelist"
                    )
                if self.tq.enable_proxy and not self.proxy.effective_http:
                    raise ValueError(
                        "proxy.http must be configured when tq.enable_proxy is true"
                    )
                continue
            if item.service == "ctp":
                account = (
                    None
                    if self.ctp is None
                    else (self.ctp.test if item.mode == "sandbox" else self.ctp.live)
                )
                if account is None:
                    raise ValueError(
                        f"missing {item.mode} ctp config referenced by service_whitelist"
                    )
                continue
            if (
                item.exchange == "kraken"
                and item.market == "spot"
                and item.mode == "sandbox"
            ):
                raise ValueError("kraken spot sandbox is not supported")
            exchange_config = getattr(self, item.exchange)
            if exchange_config is None:
                raise ValueError(
                    f"missing config for exchange '{item.exchange}' referenced by service_whitelist"
                )

            credentials = (
                exchange_config.test if item.mode == "sandbox" else exchange_config.live
            )
            if credentials is None:
                raise ValueError(
                    f"missing {item.mode} credentials for exchange '{item.exchange}'"
                )

            if (
                item.exchange == "kraken"
                and exchange_config.enable_proxy
                and not self.proxy.effective_http
            ):
                raise ValueError(
                    f"proxy.http must be configured when {item.exchange}.enable_proxy is true"
                )

        return self
