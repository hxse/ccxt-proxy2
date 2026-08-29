from typing import Any

from fastapi import APIRouter, Depends

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
    prefix="/ccxt", dependencies=[Depends(manager)], tags=["CCXT PROXY"]
)


def _client(params):
    return exchange_manager.get_client(params.exchange_name, params.market, params.mode)


def _ohlcv_response(result) -> OhlcvResponse:
    return OhlcvResponse(
        rows=result.rows,
        last_bar_completion_confirmed=result.last_bar_completion_confirmed,
    )


@ccxt_router.get("/ohlcv/since-limit", response_model=OhlcvResponse)
def fetch_ohlcv_since_limit(params: SinceLimitOhlcvRequest = Depends()):
    result = _client(params).fetch_ohlcv_since_limit(
        params.symbol,
        params.timeframe,
        params.since,
        params.limit,
        variant=params.variant,
        enable_cache=params.enable_cache,
        include_last=params.include_last,
    )
    return _ohlcv_response(result)


@ccxt_router.get("/ohlcv/since-latest", response_model=OhlcvResponse)
def fetch_ohlcv_since_latest(params: SinceLatestOhlcvRequest = Depends()):
    result = _client(params).fetch_ohlcv_since_latest(
        params.symbol,
        params.timeframe,
        params.since,
        variant=params.variant,
        enable_cache=params.enable_cache,
        include_last=params.include_last,
    )
    return _ohlcv_response(result)


@ccxt_router.get("/ohlcv/latest-limit", response_model=OhlcvResponse)
def fetch_ohlcv_latest_limit(params: LatestLimitOhlcvRequest = Depends()):
    result = _client(params).fetch_ohlcv_latest_limit(
        params.symbol,
        params.timeframe,
        params.limit,
        variant=params.variant,
        enable_cache=params.enable_cache,
        include_last=params.include_last,
    )
    return _ohlcv_response(result)


@ccxt_router.get("/fetch_balance", response_model=BalanceResponse)
def fetch_balance(params: BalanceRequest = Depends()):
    return {"balance": _client(params).fetch_balance()}


@ccxt_router.get("/fetch_tickers", response_model=TickersResponse)
def fetch_tickers(params: TickersRequest = Depends()):
    return {"tickers": _client(params).fetch_tickers(params.symbols_list)}


@ccxt_router.get("/fetch_market_info", response_model=MarketInfoResponse)
def fetch_market_info(params: MarketInfoRequest = Depends()):
    return _client(params).fetch_market_info(params.symbol)


@ccxt_router.get("/fetch_order", response_model=OrderResponse)
def fetch_order(params: FetchOrderRequest = Depends()):
    return {"order": _client(params).fetch_order(params.id, params.symbol)}


@ccxt_router.post("/create_market_order", response_model=OrderResponse)
def create_market_order(params: MarketOrderRequest):
    extra = _order_extra(params)
    order = _client(params).create_order(
        params.symbol, "market", params.side, params.amount, params=extra
    )
    return {"order": order}


@ccxt_router.post("/create_limit_order", response_model=OrderResponse)
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


@ccxt_router.post("/create_stop_market_order", response_model=OrderResponse)
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


@ccxt_router.post("/create_take_profit_market_order", response_model=OrderResponse)
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


@ccxt_router.post("/close_position", response_model=ClosePositionResponse)
def close_position(params: ClosePositionRequest):
    remaining = _client(params).close_position(
        params.symbol, params.side, params.model_extra
    )
    return {"remaining_positions": remaining}


@ccxt_router.post("/cancel_all_orders", response_model=CancelAllOrdersResponse)
def cancel_all_orders(params: CancelAllOrdersRequest):
    result = _client(params).cancel_all_orders(params.symbol, params.model_extra)
    return {"result": result}


@ccxt_router.get("/fetch_open_orders", response_model=OrdersResponse)
def fetch_open_orders(params: FetchOpenOrdersRequest = Depends()):
    orders = _client(params).fetch_open_orders(
        params.symbol, params.since, params.limit
    )
    return {"orders": orders}


@ccxt_router.get("/fetch_closed_orders", response_model=OrdersResponse)
def fetch_closed_orders(params: FetchClosedOrdersRequest = Depends()):
    orders = _client(params).fetch_closed_orders(
        params.symbol, params.since, params.limit
    )
    return {"orders": orders}


@ccxt_router.get("/fetch_my_trades", response_model=TradesResponse)
def fetch_my_trades(params: FetchMyTradesRequest = Depends()):
    trades = _client(params).fetch_my_trades(params.symbol, params.since, params.limit)
    return {"trades": trades}


@ccxt_router.get("/fetch_positions", response_model=PositionsResponse)
def fetch_positions(params: FetchPositionsRequest = Depends()):
    return {"positions": _client(params).fetch_positions(params.symbols)}


@ccxt_router.post("/set_leverage", response_model=GenericResponse)
def set_leverage(params: SetLeverageRequest):
    result = _client(params).set_leverage(
        params.leverage, params.symbol, params.model_extra
    )
    return {"result": result}


@ccxt_router.post("/set_margin_mode", response_model=GenericResponse)
def set_margin_mode(params: SetMarginModeRequest):
    result = _client(params).set_margin_mode(
        params.marginMode, params.symbol, params.model_extra
    )
    return {"result": result}


@ccxt_router.post("/cancel_order", response_model=OrderResponse)
def cancel_order(params: CancelOrderRequest):
    order = _client(params).cancel_order(params.id, params.symbol, params.model_extra)
    return {"order": order}


def _order_extra(params) -> dict[str, Any]:
    extra = dict(params.model_extra or {})
    if params.clientOrderId:
        extra["clientOrderId"] = params.clientOrderId
    return extra
