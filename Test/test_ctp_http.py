import asyncio

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.main import app as main_app
from src.router.auth_handler import manager as auth_manager
from src.router.ctp_router import ctp_router
from src.tools.config_types import AppConfig
from src.tools.ctp_manager import CtpManager
from src.types_ctp import (
    CtpCancelByExchange,
    CtpCancelBySession,
    CtpLimitOrderRequest,
    CtpMarketOrderRequest,
)
from Test.ctp_fakes import QUERY_CASES, FakeFactory, ctp_config, record

ORDER = {
    "mode": "sandbox",
    "exchange_id": "SHFE",
    "instrument_id": "rb2610",
    "side": "buy",
    "offset": "open",
    "volume": 1,
}


class LocalClient:
    def __init__(self, app):
        self.app = app

    def request(self, method, path, **kwargs):
        async def run():
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self.app), base_url="http://test"
            ) as client:
                return await client.request(method, path, **kwargs)

        return asyncio.run(run())

    def get(self, path, **kwargs):
        return self.request("GET", path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("POST", path, **kwargs)


@pytest.fixture
def http(tmp_path, monkeypatch):
    factory = FakeFactory()
    factory.setup = lambda api: api.queries.update(
        {method: [record(model)] for _, method, _, model in QUERY_CASES}
    )
    manager = CtpManager(ctp_config(tmp_path), factory)
    monkeypatch.setattr("src.router.ctp_router.ctp_manager", manager)
    app = FastAPI()
    app.include_router(ctp_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    yield LocalClient(app), factory
    manager.close()


@pytest.mark.parametrize(
    ("route", "key"),
    [
        ("fetch_orders", "orders"),
        ("fetch_trades", "trades"),
        ("fetch_positions", "positions"),
        ("fetch_balance", "accounts"),
    ],
)
def test_read_routes_return_detailed_typed_records_and_mode(http, route, key):
    client, factory = http
    response = client.get("/ctp/" + route, params={"mode": "live"})
    assert response.status_code == 200
    body = response.json()
    assert body["mode"] == "live" and body["trading_day"] == "20260917"
    assert isinstance(body["request_id"], int)
    assert len(body[key][0]) >= 25
    assert factory.apis[0].front.endswith(":10002")


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("create_market_order", ORDER),
        ("create_limit_order", ORDER | {"price": 3500}),
        (
            "cancel_order",
            {
                "mode": "sandbox",
                "exchange_id": "SHFE",
                "instrument_id": "rb2610",
                "by": "exchange_order",
                "order_sys_id": "      42",
            },
        ),
        (
            "cancel_order",
            {
                "mode": "sandbox",
                "exchange_id": "SHFE",
                "instrument_id": "rb2610",
                "by": "session_order",
                "front_id": 7,
                "session_id": 9,
                "order_ref": "11",
            },
        ),
    ],
)
def test_write_routes_return_native_order_fields(http, route, body):
    client, _ = http
    response = client.post("/ctp/" + route, json=body)
    assert response.status_code == 200, response.text
    order = response.json()["order"]
    assert len(order) >= 60
    assert order["InstrumentID"] == "rb2610"
    assert order["OrderStatus"] == ("5" if route == "cancel_order" else "3")
    assert "BrokerID" in order and "StatusMsg" in order


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("create_market_order", ORDER | {"price": 3500}),
        ("create_limit_order", ORDER),
        ("create_limit_order", ORDER | {"price": True}),
        ("create_limit_order", ORDER | {"price": 0}),
        ("create_limit_order", ORDER | {"price": "NaN"}),
        ("create_market_order", ORDER | {"volume": True}),
        ("create_market_order", ORDER | {"volume": 1.5}),
        ("create_market_order", ORDER | {"offset": "auto"}),
        ("create_market_order", ORDER | {"mode": "paper"}),
        ("create_market_order", ORDER | {"instrument_id": "KQ.m@SHFE.rb"}),
        ("create_market_order", ORDER | {"password": "never-forward"}),
        (
            "cancel_order",
            {
                "exchange_id": "SHFE",
                "instrument_id": "rb2610",
                "by": "exchange_order",
                "order_sys_id": "  ",
            },
        ),
        (
            "cancel_order",
            {
                "exchange_id": "SHFE",
                "instrument_id": "rb2610",
                "by": "session_order",
                "order_ref": "11",
            },
        ),
        (
            "cancel_order",
            {
                "exchange_id": "SHFE",
                "instrument_id": "rb2610",
                "by": "exchange_order",
                "order_sys_id": "42",
                "front_id": 1,
            },
        ),
    ],
)
def test_invalid_body_is_rejected_before_any_sdk_use(http, route, body):
    client, factory = http
    assert client.post("/ctp/" + route, json=body).status_code == 422
    assert factory.apis == []


@pytest.mark.parametrize(
    "path",
    [
        "/ctp/fetch_orders?unknown=value",
        "/ctp/fetch_trades?trade_time_start=25:00:00",
        "/ctp/fetch_positions?mode=paper",
        "/ctp/fetch_balance?password=hidden",
    ],
)
def test_strict_query_validation_precedes_sdk_use(http, path):
    client, factory = http
    assert client.get(path).status_code == 422
    assert factory.apis == []


def test_body_routes_reject_query_parameters(http):
    client, factory = http
    assert (
        client.post("/ctp/create_market_order?mode=live", json=ORDER).status_code == 422
    )
    assert factory.apis == []


def test_bearer_authentication_is_required(http):
    client, factory = http
    client.app.dependency_overrides.clear()
    assert client.get("/ctp/fetch_balance").status_code == 401
    assert factory.apis == []


def test_config_reuses_app_config_and_masks_secrets(tmp_path):
    config = ctp_config(tmp_path)
    assert AppConfig(SECRET="offline", ctp=config).ctp is config
    assert "sim-password" not in repr(config) and "test-auth" not in repr(config)
    bad = config.test.model_dump()
    bad["auth_code"] = None
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"SECRET": "offline", "ctp": {"test": bad}})


def test_docs_have_explicit_parameters_native_types_and_cancel_variants():
    schema = main_app.openapi()
    for path, operations in schema["paths"].items():
        if not path.startswith("/ctp/"):
            continue
        operation = next(iter(operations.values()))
        assert operation["security"]
        assert operation["summary"] and operation["description"]
        response_ref = operation["responses"]["200"]["content"]["application/json"][
            "schema"
        ]["$ref"]
        response_model = schema["components"]["schemas"][response_ref.rsplit("/", 1)[1]]
        assert {"mode", "request_id", "trading_day"} <= set(response_model["required"])
    models = schema["components"]["schemas"]
    assert "price" in models["CtpLimitOrderRequest"]["required"]
    assert "price" not in models["CtpMarketOrderRequest"]["properties"]
    for model in (
        CtpLimitOrderRequest,
        CtpMarketOrderRequest,
        CtpCancelByExchange,
        CtpCancelBySession,
    ):
        definition = models[model.__name__]
        assert definition["additionalProperties"] is False
        for example in definition["examples"]:
            model.model_validate(example)
        assert all(
            field.get("description") for field in definition["properties"].values()
        )
    cancel = schema["paths"]["/ctp/cancel_order"]["post"]["requestBody"]["content"][
        "application/json"
    ]["schema"]
    assert len(cancel["oneOf"]) == 2 and cancel["discriminator"]["propertyName"] == "by"
    for name in ("CtpOrder", "CtpTrade", "CtpPosition", "CtpTradingAccount"):
        assert all(
            field.get("description") for field in models[name]["properties"].values()
        )
    # 校验本路由单独注册时也没有悬空的 OpenAPI schema 引用。
    local = FastAPI()
    local.include_router(ctp_router)
    document = local.openapi()

    def check_refs(value):
        if isinstance(value, dict):
            if "$ref" in value:
                assert (
                    value["$ref"].rsplit("/", 1)[1] in document["components"]["schemas"]
                )
            for item in value.values():
                check_refs(item)
        elif isinstance(value, list):
            for item in value:
                check_refs(item)

    check_refs(document)
