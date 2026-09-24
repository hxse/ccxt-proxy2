import ccxt
import pytest
from fastapi import FastAPI

from src.domain_errors import DomainError
from src.responses import OrderResponse
from src.router.auth_handler import manager as auth_manager
from src.router.trader_router import ccxt_router
from src.tools.shared import handle_domain_error
from src.types import (
    LimitOrderRequest,
    StopMarketOrderRequest,
    TakeProfitMarketOrderRequest,
)
from Test.test_ccxt_prices import SYMBOL, price_client
from Test.test_ctp_http import LocalClient
from Test.test_ctp_http import http as http


@pytest.mark.parametrize(
    ("price", "side", "expected"),
    [
        (50000.15, "buy", 50000.1),
        (50000.100000000006, "sell", 50000.1),
    ],
)
def test_ccxt_http_returns_effective_price_and_structured_bounds(
    monkeypatch, price, side, expected
):
    client, exchange, _ = price_client()
    original = exchange.create_order

    def create(symbol, kind, side, amount, price, params):
        order = original(symbol, kind, side, amount, price, params=params)
        return {
            **order,
            "symbol": symbol,
            "status": "open",
            "type": kind,
            "side": side,
            "price": float(price),
        }

    exchange.create_order = create
    monkeypatch.setattr(
        "src.router.trader_router.exchange_manager.get_client", lambda *args: client
    )
    app = FastAPI()
    app.include_router(ccxt_router)
    app.add_exception_handler(DomainError, handle_domain_error)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    http = LocalClient(app)
    request = {
        "exchange_name": "binance",
        "market": "future",
        "mode": "sandbox",
        "symbol": SYMBOL,
        "side": side,
        "amount": 1,
        "price": price,
    }
    accepted = http.post("/ccxt/create_limit_order", json=request)
    assert accepted.status_code == 200
    assert accepted.json()["order"]["price"] == expected
    assert accepted.json()["order"]["price_adjustment"]["requested_price"] == str(price)
    rejected = http.post(
        "/ccxt/create_limit_order",
        json=request | {"price": 60000 if side == "buy" else 40000},
    )
    assert rejected.status_code == 422
    assert rejected.json()["detail"]["code"] == "PRICE_OUT_OF_RANGE"
    assert exchange.create_calls == 1
    schema = app.openapi()
    assert (
        "price_adjustment"
        in schema["components"]["schemas"]["OrderStructure"]["properties"]
    )


@pytest.mark.parametrize(
    ("price", "expected"), [(3574.2, 3575), (3574.0000000000005, 3574)]
)
def test_ctp_http_returns_price_adjustment_and_preflight_error(http, price, expected):
    client, factory = http
    request = {
        "mode": "sandbox",
        "exchange_id": "SHFE",
        "instrument_id": "rb2610",
        "side": "sell",
        "offset": "open",
        "volume": 1,
        "price": price,
    }
    response = client.post("/ctp/create_limit_order", json=request)
    assert response.status_code == 200
    assert response.json()["order"]["LimitPrice"] == expected
    assert response.json()["price_adjustment"] == {
        "requested_price": str(price),
        "submitted_price": str(expected),
        "tick_size": "1",
        "adjusted": True,
    }
    response = client.post("/ctp/create_limit_order", json=request | {"price": 100001})
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "PRICE_OUT_OF_RANGE"
    assert response.json()["detail"]["order_identity"] is None
    assert sum(m == "ReqOrderInsert" for m, _, _ in factory.apis[0].requests) == 1


@pytest.mark.parametrize(
    ("route", "field"),
    [
        ("create_limit_order", "price"),
        ("create_stop_market_order", "triggerPrice"),
        ("create_take_profit_market_order", "triggerPrice"),
    ],
)
@pytest.mark.parametrize("value", [True, False])
def test_boolean_prices_fail_at_http_validation_before_client(
    monkeypatch, route, field, value
):
    def no_client(*args):
        pytest.fail("非法布尔值价格不能进入客户端")

    monkeypatch.setattr(
        "src.router.trader_router.exchange_manager.get_client", no_client
    )
    app = FastAPI()
    app.include_router(ccxt_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    response = LocalClient(app).post(
        "/ccxt/" + route,
        json={
            "exchange_name": "binance",
            "market": "future",
            "mode": "sandbox",
            "symbol": SYMBOL,
            "side": "buy",
            "amount": 1,
            field: value,
        },
    )
    assert response.status_code == 422
    issue = response.json()["detail"][0]
    assert issue["loc"] == ["body", field]
    assert issue["type"] == "value_error" and "布尔值" in issue["msg"]


@pytest.mark.parametrize(
    ("model", "field"),
    [
        (LimitOrderRequest, "price"),
        (StopMarketOrderRequest, "triggerPrice"),
        (TakeProfitMarketOrderRequest, "triggerPrice"),
    ],
)
@pytest.mark.parametrize("value", [100, 100.1, "100.10"])
def test_numeric_prices_keep_existing_input_compatibility(model, field, value):
    request = model(
        exchange_name="binance",
        market="future",
        symbol=SYMBOL,
        side="buy",
        amount=1,
        **{field: value},
    )
    assert getattr(request, field) == float(value)


def test_kraken_spot_ack_retains_unknown_status_and_price_adjustment():
    sdk = ccxt.kraken()
    sdk.set_markets(
        [
            {
                "id": "ETHUSDT",
                "symbol": "ETH/USDT",
                "base": "ETH",
                "quote": "USDT",
                "type": "spot",
                "spot": True,
                "contract": False,
                "precision": {"price": 0.01, "amount": 0.0001},
                "info": {"altname": "ETHUSDT"},
            }
        ]
    )
    parsed = sdk.parse_order(
        {
            "descr": {"order": "buy 1.00000000 ETHUSDT @ limit 330.12"},
            "txid": ["order-1"],
        }
    )
    parsed["price_adjustment"] = {
        "requested_price": "330.129",
        "submitted_price": "330.12",
        "tick_size": "0.01",
        "adjusted": True,
    }
    response = OrderResponse.model_validate({"order": parsed})
    assert response.order.id == "order-1" and response.order.status is None
    assert response.order.price_adjustment.submitted_price == "330.12"
