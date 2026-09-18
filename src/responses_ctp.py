"""CTP HTTP 返回详细原生记录，不构造 CCXT unified order。"""

from pydantic import BaseModel, ConfigDict, Field

from src.base_types import ModeType
from src.ctp_records_account import CtpPosition, CtpTradingAccount
from src.ctp_records_trading import CtpOrder, CtpTrade
from src.responses_system import ServiceErrorDetail
from src.responses_trading_status import TradingStatusResponse


class CtpInstrumentStatus(BaseModel):
    """OnRtnInstrumentStatus 的完整有效字段，状态按品种推送。"""

    model_config = ConfigDict(extra="forbid")

    ExchangeID: str = Field(description="交易所代码。")
    InstrumentID: str = Field(description="状态推送中的品种代码，对应请求 product_id。")
    ExchangeInstID: str = Field(description="交易所原始合约/品种代码。")
    SettlementGroupID: str = Field(description="结算组代码，可能为空。")
    InstrumentStatus: str = Field(
        description="0=开盘前，1=非交易，2=连续交易，3=集合竞价报单，4=集合竞价价格平衡，5=集合竞价撮合，6=收盘，7=交易处理中；未知编码原样保留。"
    )
    TradingSegmentSN: int = Field(description="上游交易阶段编号。")
    EnterTime: str = Field(
        description="进入该状态的上游时间 HH:MM:SS，无日期；不是 HTTP 查询时间，也不用于超时失效判断。"
    )
    EnterReason: str = Field(
        description="进入原因：1=自动切换，2=手动切换，3=熔断；保留原始编码。"
    )


class CtpTradingStatusResponse(TradingStatusResponse):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mode": "sandbox",
                    "exchange_id": "SHFE",
                    "product_id": "rb",
                    "is_open": True,
                    "raw_status": "2",
                    "reason": None,
                    "data": {
                        "ExchangeID": "SHFE",
                        "InstrumentID": "rb",
                        "ExchangeInstID": "rb",
                        "SettlementGroupID": "",
                        "InstrumentStatus": "2",
                        "TradingSegmentSN": 1,
                        "EnterTime": "09:00:00",
                        "EnterReason": "1",
                    },
                },
                {
                    "mode": "sandbox",
                    "exchange_id": "SHFE",
                    "product_id": "rb",
                    "is_open": None,
                    "raw_status": None,
                    "reason": "not_received",
                    "data": None,
                },
            ]
        }
    )

    mode: ModeType = Field(
        description="实际使用的 sandbox/live；模拟状态仅代表对应模拟前置。"
    )
    exchange_id: str = Field(description="本次查询的交易所代码。", examples=["SHFE"])
    product_id: str = Field(description="本次查询的品种代码。", examples=["rb"])
    raw_status: str | None = Field(
        None,
        description="有效通知中的 InstrumentStatus：仅 2 表示连续交易，0/1/3/4/5/6/7 为其他阶段；未知编码保留并返回 is_open=null。",
        examples=["2", "6", None],
    )
    data: CtpInstrumentStatus | None = Field(
        None,
        description="当前连接最新的 OnRtnInstrumentStatus 快照；断线或未收到时为 null。状态推送没有 nRequestID，不伪造请求号或交易日。",
    )


class CtpResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: ModeType = Field(description="本次请求实际使用的 sandbox/live 配置。")
    request_id: int = Field(
        description="CTP nRequestID，与 HTTP X-Request-ID 不同；只在此连接内关联请求。"
    )
    trading_day: str = Field(
        description="登录回报的 CTP 交易日 YYYYMMDD，不是服务器自然日。"
    )


class CtpOrderResponse(CtpResponse):
    order: CtpOrder = Field(
        description="OnRtnOrder 订单快照；下单不保证已成交，撤单成功确认 OrderStatus=5。"
    )


class CtpOrdersResponse(CtpResponse):
    orders: list[CtpOrder] = Field(
        description="OnRspQryOrder 的全部记录；收到 bIsLast 后返回，无记录时为 []。"
    )


class CtpTradesResponse(CtpResponse):
    trades: list[CtpTrade] = Field(
        description="OnRspQryTrade 的全部逐笔成交，不按订单聚合；无记录时为 []。"
    )


class CtpPositionsResponse(CtpResponse):
    positions: list[CtpPosition] = Field(
        description="OnRspQryInvestorPosition 原始持仓行，保留多空、今昨、套保及冻结数量；无记录时为 []。"
    )


class CtpAccountsResponse(CtpResponse):
    accounts: list[CtpTradingAccount] = Field(
        description="OnRspQryTradingAccount 全部资金账户行；金额币种见各行 CurrencyID，无记录时为 []。"
    )


class CtpOrderIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    exchange_id: str = Field(description="订单的 ExchangeID。")
    instrument_id: str = Field(description="订单的 InstrumentID。")
    order_ref: str | None = Field(
        None, description="OrderRef，结合 front_id/session_id 定位。"
    )
    front_id: int | None = Field(None, description="原订单 FrontID。")
    session_id: int | None = Field(None, description="原订单 SessionID。")
    order_sys_id: str | None = Field(
        None, description="已知时的 OrderSysID，保留前导空格。"
    )


class CtpErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str = Field(
        description="稳定错误码，如 CTP_ORDER_REJECTED、CTP_TIMEOUT、OPERATION_STATUS_UNKNOWN。"
    )
    message: str = Field(
        description="错误说明或上游 ErrorMsg/StatusMsg，已对配置凭证脱敏。"
    )
    mode: ModeType = Field(description="本次请求的 sandbox/live 模式。")
    request_id: int | None = Field(None, description="已分配时的 CTP nRequestID。")
    return_code: int | None = Field(
        None, description="Req* 的非零本地返回码；表示本次未成功发送。"
    )
    ctp_error_id: int | None = Field(
        None,
        description="上游 RspInfo.ErrorID；仅有 StatusMsg 的拒单回报或本地错误时为 null。",
    )
    order_identity: CtpOrderIdentity | None = Field(
        None, description="已知订单定位信息；操作状态未知时用于先查询订单/成交对账。"
    )


class CtpErrorResponse(BaseModel):
    detail: CtpErrorDetail | ServiceErrorDetail = Field(
        description="CTP 业务/连接错误；HTTP 参数校验另用 FastAPI HTTPValidationError。"
    )


class CtpValidationIssue(BaseModel):
    loc: list[str | int] = Field(description="错误位置，如 body/price 或 query/mode。")
    msg: str = Field(description="参数校验错误说明。")
    type: str = Field(description="Pydantic/FastAPI 校验错误类型。")


class CtpValidationResponse(BaseModel):
    detail: CtpErrorDetail | list[CtpValidationIssue] = Field(
        description="上游拒单的结构化详情，或 FastAPI 参数校验错误列表。"
    )
