from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query

from src.responses import (
    BalanceResponse,
    CancelAllOrdersResponse,
    ClosePositionResponse,
    GenericResponse,
    MarketInfoResponse,
    OhlcvResponse,
    OrderResponse,
    OrdersResponse,
    PositionsResponse,
    TickersResponse,
    TradesResponse,
)
from src.router.auth_handler import manager
from src.router.ccxt_docs import (
    CANCEL_ALL_ORDERS_DESCRIPTION,
    CCXT_ORDER_READ_RESPONSES,
    CCXT_READ_RESPONSES,
    CCXT_WRITE_RESPONSES,
    CLOSE_POSITION_DESCRIPTION,
    OHLCV_RESPONSES,
)
from src.router.query_validation import reject_query_params_on_non_get
from src.tools.exchange_manager import exchange_manager
from src.types import (
    BalanceRequest,
    CancelAllOrdersRequest,
    CancelOrderRequest,
    ClosePositionRequest,
    FetchClosedOrdersRequest,
    FetchMyTradesRequest,
    FetchOpenOrdersRequest,
    FetchOrderRequest,
    FetchPositionsRequest,
    LatestLimitOhlcvRequest,
    LimitOrderRequest,
    MarketInfoRequest,
    MarketOrderRequest,
    SetLeverageRequest,
    SetMarginModeRequest,
    SinceLatestOhlcvRequest,
    SinceLimitOhlcvRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
    TickersRequest,
)

ccxt_router = APIRouter(
    prefix="/ccxt",
    dependencies=[Depends(manager), Depends(reject_query_params_on_non_get)],
    tags=["CCXT PROXY"],
)


def _client(params):
    return exchange_manager.get_client(params.exchange_name, params.market, params.mode)


def _ohlcv_response(result) -> OhlcvResponse:
    return OhlcvResponse(
        rows=result.rows,
        last_bar_completion_confirmed=result.last_bar_completion_confirmed,
    )


@ccxt_router.get(
    "/fetch_ohlcv/since-limit",
    response_model=OhlcvResponse,
    summary="从指定时间获取固定数量 K 线",
    description="从 since 向后返回最多 limit 根完整 rows；可复用一个最佳 cache prefix，并通过 successor metadata 描述尾根证据。",
    response_description="完整目标 rows 与 last_bar_completion_confirmed。",
    responses=OHLCV_RESPONSES,
)
def fetch_ohlcv_since_limit(params: Annotated[SinceLimitOhlcvRequest, Query()]):
    result = _client(params).fetch_ohlcv_since_limit(
        params.symbol,
        params.timeframe,
        params.since,
        params.limit,
        variant=params.variant,
        enable_cache=params.enable_cache,
    )
    return _ohlcv_response(result)


@ccxt_router.get(
    "/fetch_ohlcv/since-latest",
    response_model=OhlcvResponse,
    summary="从指定时间获取到最新快照的 K 线",
    description="请求开始时冻结 latest snapshot，从 since 分页追到该快照；最多返回 100,000 根，不追赶分页期间新产生的 K 线。",
    response_description="从 since 到固定 snapshot 的完整 rows 与尾根证据。",
    responses=OHLCV_RESPONSES,
)
def fetch_ohlcv_since_latest(params: Annotated[SinceLatestOhlcvRequest, Query()]):
    result = _client(params).fetch_ohlcv_since_latest(
        params.symbol,
        params.timeframe,
        params.since,
        variant=params.variant,
        enable_cache=params.enable_cache,
    )
    return _ohlcv_response(result)


@ccxt_router.get(
    "/fetch_ohlcv/latest-limit",
    response_model=OhlcvResponse,
    summary="获取最新倒数固定数量 K 线",
    description="返回 Provider 最新倒数 limit 根。该模式不读取 cache，但会将可安全复用的非尾根写入 cache。",
    response_description="完整 latest-limit rows；非空结果的尾根证据保守为 false。",
    responses=OHLCV_RESPONSES,
)
def fetch_ohlcv_latest_limit(params: Annotated[LatestLimitOhlcvRequest, Query()]):
    result = _client(params).fetch_ohlcv_latest_limit(
        params.symbol,
        params.timeframe,
        params.limit,
        variant=params.variant,
        enable_cache=params.enable_cache,
    )
    return _ohlcv_response(result)


@ccxt_router.get(
    "/fetch_balance",
    response_model=BalanceResponse,
    summary="获取账户余额",
    description="只读调用指定 CCXT identity 的 unified balance API，返回 free/used/total 以及 Provider 扩展字段。",
    response_description="CCXT unified balance。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_balance(params: Annotated[BalanceRequest, Query()]):
    return {"balance": _client(params).fetch_balance()}


@ccxt_router.get(
    "/fetch_tickers",
    response_model=TickersResponse,
    summary="批量获取行情报价",
    description="只读获取逗号分隔 symbols 的 CCXT unified tickers；不传 symbols 时由 Provider 决定返回范围。",
    response_description="以 canonical symbol 为 key 的 ticker 字典。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_tickers(params: Annotated[TickersRequest, Query()]):
    return {"tickers": _client(params).fetch_tickers(params.symbols_list)}


@ccxt_router.get(
    "/fetch_market_info",
    response_model=MarketInfoResponse,
    summary="获取下单所需市场信息",
    description="返回 linear、settle、amount step、最小数量和 contract size；只有 position 明确提供时 leverage 才为整数，否则为 null。",
    response_description="稳定的市场精度、限额和可确认杠杆信息。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_market_info(params: Annotated[MarketInfoRequest, Query()]):
    return _client(params).fetch_market_info(params.symbol)


@ccxt_router.get(
    "/fetch_order",
    response_model=OrderResponse,
    summary="按 ID 查询订单",
    description="只读查询指定订单。Binance Futures normal endpoint 明确不存在时会 fallback 到 conditional/stop endpoint。",
    response_description="CCXT unified order。",
    responses=CCXT_ORDER_READ_RESPONSES,
)
def fetch_order(params: Annotated[FetchOrderRequest, Query()]):
    return {"order": _client(params).fetch_order(params.id, params.symbol)}


@ccxt_router.post(
    "/create_market_order",
    response_model=OrderResponse,
    summary="创建市价单",
    description="有状态操作：按 side/amount 创建 market order；Provider 扩展参数和 clientOrderId 会由 CcxtClient 统一翻译。网络失败不会自动重试。",
    response_description="Provider 接受后返回的 unified order。",
    responses=CCXT_WRITE_RESPONSES,
)
def create_market_order(params: MarketOrderRequest):
    extra = _order_extra(params)
    order = _client(params).create_order(
        params.symbol, "market", params.side, params.amount, params=extra
    )
    return {"order": order}


@ccxt_router.post(
    "/create_limit_order",
    response_model=OrderResponse,
    summary="创建限价单",
    description="有状态操作：按 price/amount 创建 limit order，可传 timeInForce、postOnly、clientOrderId 和 Provider 扩展参数。",
    response_description="Provider 接受后返回的 unified order。",
    responses=CCXT_WRITE_RESPONSES,
)
def create_limit_order(params: LimitOrderRequest):
    extra = _order_extra(params)
    if params.timeInForce:
        extra["timeInForce"] = params.timeInForce
    if params.postOnly:
        extra["postOnly"] = True
    order = _client(params).create_order(
        params.symbol, "limit", params.side, params.amount, params.price, extra
    )
    return {"order": order}


@ccxt_router.post(
    "/create_stop_market_order",
    response_model=OrderResponse,
    summary="创建止损市价触发单",
    description="有状态操作：将 triggerPrice 翻译为 Provider stop-loss trigger；默认 reduceOnly=true，网络失败不会自动重试。",
    response_description="创建后的 unified conditional order。",
    responses=CCXT_WRITE_RESPONSES,
)
def create_stop_market_order(params: StopMarketOrderRequest):
    order = _client(params).create_stop_market_order(
        params.symbol,
        params.side,
        params.amount,
        params.triggerPrice,
        reduce_only=params.reduceOnly,
        client_order_id=params.clientOrderId,
        time_in_force=params.timeInForce,
        params=params.model_extra,
    )
    return {"order": order}


@ccxt_router.post(
    "/create_take_profit_market_order",
    response_model=OrderResponse,
    summary="创建止盈市价触发单",
    description="有状态操作：将 triggerPrice 翻译为 Provider take-profit trigger；默认 reduceOnly=true，网络失败不会自动重试。",
    response_description="创建后的 unified conditional order。",
    responses=CCXT_WRITE_RESPONSES,
)
def create_take_profit_market_order(params: TakeProfitMarketOrderRequest):
    order = _client(params).create_take_profit_market_order(
        params.symbol,
        params.side,
        params.amount,
        params.triggerPrice,
        reduce_only=params.reduceOnly,
        client_order_id=params.clientOrderId,
        time_in_force=params.timeInForce,
        params=params.model_extra,
    )
    return {"order": order}


@ccxt_router.post(
    "/close_position",
    response_model=ClosePositionResponse,
    summary="按市价平仓",
    description=CLOSE_POSITION_DESCRIPTION,
    response_description="提交平仓后再次查询得到的剩余 positions。",
    responses=CCXT_WRITE_RESPONSES,
)
def close_position(params: ClosePositionRequest):
    remaining = _client(params).close_position(
        params.symbol, params.side, params.model_extra
    )
    return {"remaining_positions": remaining}


@ccxt_router.post(
    "/cancel_all_orders",
    response_model=CancelAllOrdersResponse,
    summary="撤销全部挂单",
    description=CANCEL_ALL_ORDERS_DESCRIPTION,
    response_description="Provider 返回的撤单结果。",
    responses=CCXT_WRITE_RESPONSES,
)
def cancel_all_orders(params: CancelAllOrdersRequest):
    result = _client(params).cancel_all_orders(params.symbol, params.model_extra)
    return {"result": result}


@ccxt_router.get(
    "/fetch_open_orders",
    response_model=OrdersResponse,
    summary="查询当前挂单",
    description="只读查询 open orders；Binance Futures 会合并 normal 与 conditional 结果并按 order ID 去重。",
    response_description="按时间倒序的 unified open orders。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_open_orders(params: Annotated[FetchOpenOrdersRequest, Query()]):
    orders = _client(params).fetch_open_orders(
        params.symbol, params.since, params.limit
    )
    return {"orders": orders}


@ccxt_router.get(
    "/fetch_closed_orders",
    response_model=OrdersResponse,
    summary="查询历史已关闭订单",
    description="只读查询 closed/canceled order history；Binance Futures 会合并 normal 与 conditional 结果。",
    response_description="Unified closed order 列表。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_closed_orders(params: Annotated[FetchClosedOrdersRequest, Query()]):
    orders = _client(params).fetch_closed_orders(
        params.symbol, params.since, params.limit
    )
    return {"orders": orders}


@ccxt_router.get(
    "/fetch_my_trades",
    response_model=TradesResponse,
    summary="查询账户成交历史",
    description="只读查询当前账户的 CCXT unified trades，可按 symbol、since 和 limit 缩小范围。",
    response_description="Unified trade 列表。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_my_trades(params: Annotated[FetchMyTradesRequest, Query()]):
    trades = _client(params).fetch_my_trades(params.symbol, params.since, params.limit)
    return {"trades": trades}


@ccxt_router.get(
    "/fetch_positions",
    response_model=PositionsResponse,
    summary="查询合约持仓",
    description="只读查询全部或指定 symbols 的 unified positions；symbol 会先通过 market-scope capability 校验。",
    response_description="Unified position 列表。",
    responses=CCXT_READ_RESPONSES,
)
def fetch_positions(params: Annotated[FetchPositionsRequest, Query()]):
    return {"positions": _client(params).fetch_positions(params.symbols)}


@ccxt_router.post(
    "/set_leverage",
    response_model=GenericResponse,
    summary="设置账户杠杆",
    description="有状态操作：设置指定 symbol/Provider scope 的 leverage；网络失败不会自动重试，Provider 冲突返回稳定错误。",
    response_description="Provider 的杠杆设置结果。",
    responses=CCXT_WRITE_RESPONSES,
)
def set_leverage(params: SetLeverageRequest):
    result = _client(params).set_leverage(
        params.leverage, params.symbol, params.model_extra
    )
    return {"result": result}


@ccxt_router.post(
    "/set_margin_mode",
    response_model=GenericResponse,
    summary="设置保证金模式",
    description="有状态操作：把 marginMode 设置为 cross 或 isolated；已有持仓/当前状态冲突由 Provider 返回并映射为稳定错误。",
    response_description="Provider 的保证金模式设置结果。",
    responses=CCXT_WRITE_RESPONSES,
)
def set_margin_mode(params: SetMarginModeRequest):
    result = _client(params).set_margin_mode(
        params.marginMode, params.symbol, params.model_extra
    )
    return {"result": result}


@ccxt_router.post(
    "/cancel_order",
    response_model=OrderResponse,
    summary="按 ID 撤销订单",
    description="有状态操作：撤销指定订单。Binance Futures normal endpoint 明确不存在时会 fallback 到 conditional endpoint；网络失败不重试。",
    response_description="撤销后的 unified order。",
    responses=CCXT_WRITE_RESPONSES,
)
def cancel_order(params: CancelOrderRequest):
    order = _client(params).cancel_order(params.id, params.symbol, params.model_extra)
    return {"order": order}


def _order_extra(params) -> dict[str, Any]:
    extra = dict(params.model_extra or {})
    if params.clientOrderId:
        extra["clientOrderId"] = params.clientOrderId
    return extra
