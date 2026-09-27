import pytest
from fastapi import FastAPI

from src.domain_errors import InvalidProviderRequest
from src.router.auth_handler import manager as auth_manager
from src.router.trader_router import ccxt_router
from src.types import (
    LimitOrderRequest,
    MarketOrderRequest,
    SetLeverageRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
)
from Test.helpers.ccxt_order_sdk import kraken_send_reply, sdk_client
from Test.test_ctp_http import LocalClient

BASE = {
    "exchange_name": "kraken",
    "market": "future",
    "mode": "live",
    "symbol": "BTC/USD:USD",
}
ORDERS = [
    ("create_market_order", MarketOrderRequest, {}),
    ("create_limit_order", LimitOrderRequest, {"price": 50000}),
    ("create_stop_market_order", StopMarketOrderRequest, {"triggerPrice": 49000}),
    (
        "create_take_profit_market_order",
        TakeProfitMarketOrderRequest,
        {"triggerPrice": 51000},
    ),
]


@pytest.fixture
def reject_client_lookup(monkeypatch):
    def forbidden(*args):
        pytest.fail("布尔输入必须在客户端查找及 SDK 调用之前拒绝")

    monkeypatch.setattr(
        "src.router.trader_router.exchange_manager.get_client", forbidden
    )
    app = FastAPI()
    app.include_router(ccxt_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    return LocalClient(app)


@pytest.mark.parametrize("value", [True, False])
@pytest.mark.parametrize("path,model,extra", ORDERS)
def test_boolean_amount_rejected_before_client(
    reject_client_lookup, value, path, model, extra
):
    response = reject_client_lookup.post(
        "/ccxt/" + path, json=BASE | {"side": "buy", "amount": value} | extra
    )
    assert response.status_code == 422
    assert any(
        error["loc"] == ["body", "amount"] and error["type"] == "value_error"
        for error in response.json()["detail"]
    )


@pytest.mark.parametrize("value", [True, False])
def test_boolean_leverage_rejected_before_client(reject_client_lookup, value):
    response = reject_client_lookup.post(
        "/ccxt/set_leverage", json=BASE | {"leverage": value}
    )
    assert response.status_code == 422
    assert any(
        error["loc"] == ["body", "leverage"] and error["type"] == "value_error"
        for error in response.json()["detail"]
    )


@pytest.mark.parametrize("value", [1, 0.0015, "0.0015"])
@pytest.mark.parametrize("path,model,extra", ORDERS)
def test_amount_numbers_and_strings_keep_existing_coercion(value, path, model, extra):
    request = model.model_validate(BASE | {"side": "buy", "amount": value} | extra)
    assert request.amount == float(value)


@pytest.mark.parametrize("value", [2, 2.0, "2"])
def test_leverage_numeric_forms_keep_existing_coercion(value):
    assert SetLeverageRequest.model_validate(BASE | {"leverage": value}).leverage == 2


@pytest.mark.parametrize("value", [True, False])
def test_direct_client_boolean_rejection_remains(monkeypatch, value):
    client, calls, symbol = sdk_client(monkeypatch, "kraken", kraken_send_reply)
    try:
        with pytest.raises(InvalidProviderRequest, match="amount"):
            client.create_order(symbol, "market", "buy", value)
        with pytest.raises(InvalidProviderRequest, match="leverage"):
            client.set_leverage(value, symbol)
        assert calls == []
    finally:
        client.close()
