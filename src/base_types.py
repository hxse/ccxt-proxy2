from typing import Literal
from pydantic import BaseModel

# === Enums / Literals ===
ExchangeName = Literal["binance", "kraken"]
MarketType = Literal["future", "spot"]
ModeType = Literal["sandbox", "live"]
OrderType = Literal["market", "limit", "STOP_MARKET", "TAKE_PROFIT_MARKET"]
SideType = Literal["buy", "sell"]
PositionSide = Literal["long", "short"]
VALID_PERIODS = Literal["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]


# === Base Request Models ===
class BaseExchangeRequest(BaseModel):
    """基础请求包含交易所、市场类型和模式"""

    exchange_name: ExchangeName
    market: MarketType
    mode: ModeType = "sandbox"


class BaseSymbolRequest(BaseExchangeRequest):
    """在基础请求之上增加 symbol"""

    symbol: str
