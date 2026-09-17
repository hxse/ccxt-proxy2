"""CTP 请求模型；账户和密码只从 config.toml 读取。"""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from src.base_types import ModeType, SideType

CtpExchange = Literal["SHFE", "INE", "DCE", "CZCE", "CFFEX", "GFEX"]
CtpInstrument = Annotated[
    str, StringConstraints(pattern=r"^[A-Za-z][A-Za-z0-9]{0,79}$")
]
CtpOrderRef = Annotated[str, StringConstraints(pattern=r"^[0-9]{1,12}$")]
CtpIdentifier = Annotated[
    str, StringConstraints(min_length=1, max_length=20, pattern=r"^[ -~]*[!-~][ -~]*$")
]
CtpTime = Annotated[
    str, StringConstraints(pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$")
]


class CtpRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: ModeType = Field(
        "sandbox",
        description="sandbox 使用 config.toml 的 [ctp.test]（如 SimNow），live 使用 [ctp.live]；缺少配置时拒绝，不回退。",
        examples=["sandbox"],
    )


class CtpOrderRequest(CtpRequest):
    exchange_id: CtpExchange = Field(
        description="CTP ExchangeID 交易所代码。", examples=["SHFE"]
    )
    instrument_id: CtpInstrument = Field(
        description="具体期货合约 InstrumentID，例如 rb2610；不带 SHFE. 前缀，不接受 TQ 主连或指数代码。",
        examples=["rb2610"],
    )
    side: SideType = Field(
        description="Direction：buy=0 买，sell=1 卖。开多 buy+open，开空 sell+open；平多 sell+close*，平空 buy+close*。",
        examples=["buy"],
    )
    offset: Literal["open", "close", "close_today", "close_yesterday"] = Field(
        description="CombOffsetFlag：open=0 开仓、close=1 平仓、close_today=3 平今、close_yesterday=4 平昨。由调用方按交易所规则选择。",
        examples=["open"],
    )
    volume: int = Field(
        strict=True,
        ge=1,
        le=2_147_483_647,
        description="VolumeTotalOriginal：正整数手数。",
        examples=[1],
    )
    hedge_flag: Literal["speculation", "arbitrage", "hedge"] = Field(
        "speculation",
        description="CombHedgeFlag：speculation=1 投机、arbitrage=2 套利、hedge=3 套保；上游校验权限。",
    )
    invest_unit_id: str = Field(
        "",
        max_length=16,
        pattern=r"^[!-~]*$",
        description="可选 InvestUnitID 投资单元代码，默认空。",
    )


ORDER_EXAMPLE = {
    "mode": "sandbox",
    "exchange_id": "SHFE",
    "instrument_id": "rb2610",
    "side": "buy",
    "offset": "open",
    "volume": 1,
}


class CtpMarketOrderRequest(CtpOrderRequest):
    """原生市价 AnyPrice + IOC + AV；不接受 price。"""

    model_config = ConfigDict(json_schema_extra={"examples": [ORDER_EXAMPLE]})


class CtpLimitOrderRequest(CtpOrderRequest):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [{**ORDER_EXAMPLE, "price": 3500, "time_in_force": "GFD"}]
        }
    )

    price: float = Field(
        gt=0,
        allow_inf_nan=False,
        description="LimitPrice：必填委托价格，按合约报价单位填写；上游校验价格步长和涨跌停限制。",
        examples=[3500.0],
    )
    time_in_force: Literal["GFD", "IOC", "FOK"] = Field(
        "GFD",
        description="GFD=当日有效；IOC=立即成交、剩余撤销；FOK=立即全部成交否则全部撤销。上游决定是否支持。",
    )

    @field_validator("price", mode="before")
    @classmethod
    def price_is_number(cls, value):
        if isinstance(value, bool):
            raise ValueError("price must be a number, not a boolean")
        return value


class CtpCancelBase(CtpRequest):
    exchange_id: CtpExchange = Field(
        description="原订单 ExchangeID。", examples=["SHFE"]
    )
    instrument_id: CtpInstrument = Field(
        description="原订单具体合约 InstrumentID。", examples=["rb2610"]
    )
    invest_unit_id: str = Field(
        "",
        max_length=16,
        pattern=r"^[!-~]*$",
        description="原订单 InvestUnitID，默认空。",
    )


class CtpCancelByExchange(CtpCancelBase):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mode": "sandbox",
                    "by": "exchange_order",
                    "exchange_id": "SHFE",
                    "instrument_id": "rb2610",
                    "order_sys_id": "       12345",
                }
            ]
        }
    )

    by: Literal["exchange_order"] = Field(description="使用交易所报单编号定位。")
    order_sys_id: CtpIdentifier = Field(
        description="原样复制 OrderSysID，保留前导空格，不转整数。",
        examples=["       12345"],
    )


class CtpCancelBySession(CtpCancelBase):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mode": "sandbox",
                    "by": "session_order",
                    "exchange_id": "SHFE",
                    "instrument_id": "rb2610",
                    "front_id": 1,
                    "session_id": 123,
                    "order_ref": "1",
                }
            ]
        }
    )

    by: Literal["session_order"] = Field(
        description="使用原订单 FrontID + SessionID + OrderRef 定位。"
    )
    front_id: int = Field(
        strict=True,
        ge=0,
        le=2_147_483_647,
        description="原订单 FrontID，不是重新登录后的前置编号。",
    )
    session_id: int = Field(
        strict=True,
        ge=-2_147_483_648,
        le=2_147_483_647,
        description="原订单 SessionID，不是重新登录后的会话编号。",
    )
    order_ref: CtpOrderRef = Field(description="原订单 OrderRef，保持数字字符串。")


class CtpPositionQuery(CtpRequest):
    exchange_id: CtpExchange | None = Field(
        None, description="可选 ExchangeID，省略时不限制交易所。"
    )
    instrument_id: CtpInstrument | None = Field(
        None, description="可选具体合约 InstrumentID，省略时不限制合约。"
    )
    invest_unit_id: str = Field(
        "",
        max_length=16,
        pattern=r"^[!-~]*$",
        description="可选 InvestUnitID，默认空。",
    )


class CtpOrderQuery(CtpPositionQuery):
    order_sys_id: CtpIdentifier | None = Field(
        None, description="可选 OrderSysID，保持前导空格；通常配合 exchange_id。"
    )
    insert_time_start: CtpTime | None = Field(
        None,
        description="InsertTimeStart，HH:MM:SS；CTP 当前交易日的报单时间过滤，不是历史日期。",
    )
    insert_time_end: CtpTime | None = Field(
        None, description="InsertTimeEnd，HH:MM:SS；不转换时区，夜盘语义由上游处理。"
    )


class CtpTradeQuery(CtpPositionQuery):
    trade_id: CtpIdentifier | None = Field(
        None, description="可选 TradeID，保持原始字符串；通常配合 exchange_id。"
    )
    trade_time_start: CtpTime | None = Field(
        None, description="TradeTimeStart，HH:MM:SS；过滤 CTP 当前可查询交易日的成交。"
    )
    trade_time_end: CtpTime | None = Field(
        None, description="TradeTimeEnd，HH:MM:SS；不转换时区，夜盘语义由上游处理。"
    )


class CtpAccountQuery(CtpRequest):
    currency_id: str = Field(
        "CNY",
        pattern=r"^[A-Z]{3}$",
        description="CurrencyID，默认 CNY；响应金额按各行 CurrencyID 计价。",
    )
