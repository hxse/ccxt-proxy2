from typing import Literal
from pydantic import BaseModel, Field

# === Enums / Literals ===
ExchangeName = Literal["binance", "kraken"]
MarketType = Literal["future", "spot"]
ModeType = Literal["sandbox", "live"]
OrderType = Literal["market", "limit", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
SideType = Literal["buy", "sell"]
PositionSide = Literal["long", "short"]
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


# === Base Request Models ===
class BaseExchangeRequest(BaseModel):
    """基础请求包含交易所、市场类型和模式"""

    exchange_name: ExchangeName = Field(
        ...,
        title="交易所名称",
        description="binance 或 kraken",
        examples=["binance", "kraken"],
    )
    market: MarketType = Field(
        ...,
        title="市场类型",
        description="future (合约) 或 spot (现货)",
        examples=["future", "spot"],
    )
    mode: ModeType = Field(
        "sandbox",
        title="模式",
        description="sandbox (测试网) 或 live (实盘)",
        examples=["sandbox", "live"],
    )


class BaseSymbolRequest(BaseExchangeRequest):
    """在基础请求之上增加 symbol"""

    symbol: str = Field(..., title="交易对", examples=["BTC/USDT"])
