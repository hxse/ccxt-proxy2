"""CTP HTTP 返回详细原生记录，不构造 CCXT unified order。"""

from pydantic import BaseModel, ConfigDict, Field

from src.base_types import ModeType
from src.ctp_records_account import CtpPosition, CtpTradingAccount
from src.ctp_records_trading import CtpOrder, CtpTrade


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
    detail: CtpErrorDetail = Field(
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
