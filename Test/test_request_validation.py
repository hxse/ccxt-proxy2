import math

import pytest
from pydantic import ValidationError

from src.types import (
    BalanceRequest,
    CancelAllOrdersRequest,
    CancelOrderRequest,
    ClosePositionRequest,
    FetchOpenOrdersRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    SetLeverageRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
)

EXCHANGE: dict[str, object] = {
    "exchange_name": "binance",
    "market": "future",
    "mode": "live",
}
SYMBOL = EXCHANGE | {"symbol": "BTC/USDT:USDT"}


@pytest.mark.parametrize("amount", [-1.0, 0.0, math.nan, math.inf, -math.inf])
def test_order_amount_must_be_finite_and_positive(amount):
    with pytest.raises(ValidationError):
        MarketOrderRequest.model_validate(SYMBOL | {"side": "buy", "amount": amount})


@pytest.mark.parametrize("price", [-1.0, 0.0, math.nan, math.inf, -math.inf])
def test_limit_price_must_be_finite_and_positive(price):
    with pytest.raises(ValidationError):
        LimitOrderRequest.model_validate(
            SYMBOL | {"side": "buy", "amount": 1, "price": price}
        )


@pytest.mark.parametrize(
    "request_type", [StopMarketOrderRequest, TakeProfitMarketOrderRequest]
)
def test_trigger_price_must_be_finite(request_type):
    with pytest.raises(ValidationError):
        request_type.model_validate(
            SYMBOL | {"side": "sell", "amount": 1, "triggerPrice": math.inf}
        )


@pytest.mark.parametrize("leverage", [-1, 0])
def test_leverage_must_be_positive(leverage):
    with pytest.raises(ValidationError):
        SetLeverageRequest.model_validate(
            EXCHANGE | {"symbol": "BTC/USDT:USDT", "leverage": leverage}
        )


def test_symbols_ids_limits_and_time_in_force_are_strict():
    with pytest.raises(ValidationError):
        MarketOrderRequest.model_validate(
            EXCHANGE | {"symbol": " ", "side": "buy", "amount": 1}
        )
    with pytest.raises(ValidationError):
        CancelOrderRequest.model_validate(EXCHANGE | {"id": " "})
    with pytest.raises(ValidationError):
        FetchOpenOrdersRequest.model_validate(EXCHANGE | {"limit": 0})
    with pytest.raises(ValidationError):
        FetchOpenOrdersRequest.model_validate(EXCHANGE | {"since": -1})
    with pytest.raises(ValidationError):
        LimitOrderRequest.model_validate(
            SYMBOL
            | {
                "side": "buy",
                "amount": 1,
                "price": 1,
                "timeInForce": "UNKNOWN",
            }
        )


def test_query_models_forbid_unknown_fields():
    with pytest.raises(ValidationError):
        BalanceRequest.model_validate(EXCHANGE | {"typo": True})


def test_route_models_reject_client_managed_provider_parameters():
    with pytest.raises(ValidationError, match="reduceOnly is managed internally"):
        ClosePositionRequest.model_validate(SYMBOL | {"reduceOnly": False})
    with pytest.raises(ValidationError, match="stop is managed internally"):
        CancelOrderRequest.model_validate(EXCHANGE | {"id": "1", "stop": False})
    with pytest.raises(ValidationError, match="stop is managed internally"):
        CancelAllOrdersRequest.model_validate(EXCHANGE | {"stop": False})
