"""CTP 路由只处理 HTTP schema/鉴权/转发；行情继续使用 /tq。"""

from typing import Annotated

from fastapi import APIRouter, Body, Depends, Query

from src.responses_ctp import (
    CtpAccountsResponse,
    CtpOrderResponse,
    CtpOrdersResponse,
    CtpPositionsResponse,
    CtpTradesResponse,
)
from src.router.auth_handler import manager
from src.router.ctp_docs import (
    CTP_CANCEL_DESCRIPTION,
    CTP_LIMIT_DESCRIPTION,
    CTP_MARKET_DESCRIPTION,
    CTP_READ_RESPONSES,
    CTP_WRITE_RESPONSES,
    QUERY_DESCRIPTION,
)
from src.router.query_validation import reject_query_params_on_non_get
from src.tools.ctp_manager import ctp_manager
from src.types_ctp import (
    CtpAccountQuery,
    CtpCancelByExchange,
    CtpCancelBySession,
    CtpLimitOrderRequest,
    CtpMarketOrderRequest,
    CtpOrderQuery,
    CtpPositionQuery,
    CtpTradeQuery,
)

ctp_router = APIRouter(
    prefix="/ctp",
    tags=["CTP TRADING"],
    dependencies=[Depends(manager), Depends(reject_query_params_on_non_get)],
)


@ctp_router.post(
    "/create_market_order",
    response_model=CtpOrderResponse,
    summary="CTP 市价开仓/平仓",
    description=CTP_MARKET_DESCRIPTION,
    response_description="交易回报确认接受的详细订单快照，不保证已成交。",
    responses=CTP_WRITE_RESPONSES,
)
def create_market_order(params: CtpMarketOrderRequest) -> CtpOrderResponse:
    """ReqOrderInsert：AnyPrice+IOC+AV；买卖及开平由参数指定，不查询行情。"""
    return ctp_manager.get_client(params.mode).create_order(params)


@ctp_router.post(
    "/create_limit_order",
    response_model=CtpOrderResponse,
    summary="CTP 限价开仓/平仓",
    description=CTP_LIMIT_DESCRIPTION,
    response_description="交易回报确认接受的详细订单快照，LimitPrice 为委托价格。",
    responses=CTP_WRITE_RESPONSES,
)
def create_limit_order(params: CtpLimitOrderRequest) -> CtpOrderResponse:
    """ReqOrderInsert：LimitPrice，必须填写 price；不拆分今昨仓。"""
    return ctp_manager.get_client(params.mode).create_order(params)


@ctp_router.post(
    "/cancel_order",
    response_model=CtpOrderResponse,
    summary="CTP 撤销指定订单剩余委托",
    description=CTP_CANCEL_DESCRIPTION,
    response_description="确认 OrderStatus=5 的详细订单快照，保留累计成交数量。",
    responses=CTP_WRITE_RESPONSES,
)
def cancel_order(
    params: Annotated[
        CtpCancelByExchange | CtpCancelBySession, Body(discriminator="by")
    ],
) -> CtpOrderResponse:
    """ReqOrderAction：仅 Delete，等待原订单撤单回报，不查找补充订单标识。"""
    return ctp_manager.get_client(params.mode).cancel_order(params)


@ctp_router.get(
    "/fetch_orders",
    response_model=CtpOrdersResponse,
    summary="CTP 查询订单",
    description="薄转发 `ReqQryOrder`，返回当前可查询交易日内的匹配订单，包括已成交/已撤单。"
    + QUERY_DESCRIPTION,
    response_description="mode/request_id/trading_day 与 orders 数组，每项为 CTP OrderField。",
    responses=CTP_READ_RESPONSES,
)
def fetch_orders(params: Annotated[CtpOrderQuery, Query()]) -> CtpOrdersResponse:
    """ReqQryOrder：聚合至 bIsLast，返回原始订单字段，不筛选为仅挂单。"""
    return ctp_manager.get_client(params.mode).fetch_orders(params)


@ctp_router.get(
    "/fetch_trades",
    response_model=CtpTradesResponse,
    summary="CTP 查询成交",
    description="薄转发 `ReqQryTrade`，同一订单可有多笔成交，逐笔返回。"
    + QUERY_DESCRIPTION,
    response_description="mode/request_id/trading_day 与 trades 数组，每项为 CTP TradeField。",
    responses=CTP_READ_RESPONSES,
)
def fetch_trades(params: Annotated[CtpTradeQuery, Query()]) -> CtpTradesResponse:
    """ReqQryTrade：返回完整成交分片，不合并成交或推算成交均价。"""
    return ctp_manager.get_client(params.mode).fetch_trades(params)


@ctp_router.get(
    "/fetch_positions",
    response_model=CtpPositionsResponse,
    summary="CTP 查询持仓",
    description="薄转发 `ReqQryInvestorPosition`，保留多空、今昨仓、投机套保和冻结数量维度。"
    + QUERY_DESCRIPTION,
    response_description="mode/request_id/trading_day 与 positions 数组，每项为 CTP InvestorPositionField。",
    responses=CTP_READ_RESPONSES,
)
def fetch_positions(
    params: Annotated[CtpPositionQuery, Query()],
) -> CtpPositionsResponse:
    """ReqQryInvestorPosition：原始持仓行，不合并或推算可平手数。"""
    return ctp_manager.get_client(params.mode).fetch_positions(params)


@ctp_router.get(
    "/fetch_balance",
    response_model=CtpAccountsResponse,
    summary="CTP 查询账户资金",
    description="薄转发 `ReqQryTradingAccount`，BizType=1（期货），默认 currency_id=CNY。"
    + QUERY_DESCRIPTION,
    response_description="mode/request_id/trading_day 与 accounts 数组，每项为 CTP TradingAccountField。",
    responses=CTP_READ_RESPONSES,
)
def fetch_balance(params: Annotated[CtpAccountQuery, Query()]) -> CtpAccountsResponse:
    """ReqQryTradingAccount：完整资金账户行，金额按 CurrencyID 计价。"""
    return ctp_manager.get_client(params.mode).fetch_balance(params)
