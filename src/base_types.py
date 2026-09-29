from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)

# === Enums / Literals ===
ExchangeName = Literal["binance", "kraken"]
MarketType = Literal["future", "spot"]
ModeType = Literal["sandbox", "live"]
SideType = Literal["buy", "sell"]
PositionSide = Literal["long", "short"]
NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
CCXT_TIMESTAMP_MS_MIN = 1_000_000_000_000
CCXT_TIMESTAMP_MS_MAX = 9_999_999_999_999
VALID_PERIODS = Literal[
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]


def parse_is_live(value: object) -> bool:
    """查询串和 CLI 只接受小写 true/false；不把 0/1、yes 等当作环境。"""
    if isinstance(value, bool):
        return value
    if value == "true":
        return True
    if value == "false":
        return False
    raise ValueError("is_live 必须是 true 或 false")


class EnvironmentRequest(BaseModel):
    """公开环境选择只有布尔值；内部 SDK、缓存及持久化身份保持原样。"""
    model_config = ConfigDict(extra="forbid")

    is_live: StrictBool = Field(
        description="必填：true=实盘，false=模拟盘；不提供默认环境。",
        examples=[True, False],
    )

    @property
    def mode(self) -> ModeType:
        return "live" if self.is_live else "sandbox"

    @model_validator(mode="before")
    @classmethod
    def reject_retired_environment(cls, value):
        # CCXT 写接口允许扩展参数，旧环境字段也不能借此进入上游。
        if (
            cls.model_config.get("extra") == "allow"
            and isinstance(value, dict)
            and {"mode", "trading_env"} & value.keys()
        ):
            raise ValueError("mode/trading_env 已移除，必须显式填写 is_live")
        return value


class EnvironmentQuery(EnvironmentRequest):
    @field_validator("is_live", mode="before")
    @classmethod
    def parse_query_environment(cls, value):
        return parse_is_live(value)


# === Base Request Models ===
class BaseExchangeRequest(EnvironmentRequest):
    """基础请求包含交易所、市场类型和显式环境。"""

    exchange_name: ExchangeName = Field(
        ...,
        title="交易所名称",
        description="binance 或 kraken",
        examples=["binance", "kraken"],
    )
    market: MarketType = Field(
        ...,
        title="市场类型",
        description=(
            "future (合约) 或 spot (现货)。Binance future 仅表示 "
            "USDⓈ-M linear Futures。"
        ),
        examples=["future", "spot"],
    )


class BaseSymbolRequest(BaseExchangeRequest):
    """在基础请求之上增加 symbol"""

    symbol: NonEmptyString = Field(
        ...,
        title="交易对",
        description="CCXT canonical symbol",
        examples=["BTC/USDT:USDT"],
    )
