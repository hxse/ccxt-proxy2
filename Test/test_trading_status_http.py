import pytest
from fastapi import FastAPI, HTTPException

from src.main import app as main_app
from src.responses_tq import TqTradingStatusResponse
from src.router.auth_handler import manager as auth_manager
from src.router.ctp_router import ctp_router
from src.router.tq_router import tq_router
from src.tools.ctp_manager import CtpManager
from Test.ctp_fakes import FakeFactory, ctp_config
from Test.test_ctp_http import LocalClient
from Test.test_ctp_trading_status import status_record


@pytest.fixture
def http(tmp_path, monkeypatch):
    factory = FakeFactory()
    ctp = CtpManager(ctp_config(tmp_path), factory)
    monkeypatch.setattr("src.router.ctp_router.ctp_manager", ctp)
    monkeypatch.setattr(
        "src.router.tq_router.tq_manager.fetch_trading_status",
        lambda request: TqTradingStatusResponse(
            symbol=request.symbol, reason="not_received"
        ),
    )
    app = FastAPI()
    app.include_router(tq_router)
    app.include_router(ctp_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    yield LocalClient(app), factory
    ctp.close()


def test_status_http_returns_null_and_ctp_defaults_to_sandbox(http):
    client, factory = http
    # 初始化发生在启动阶段，不由 HTTP 状态读取触发。
    from src.router.ctp_router import ctp_manager

    ctp_manager.initialize("sandbox")
    ctp_manager.initialize("live")
    tq = client.get("/tq/fetch_trading_status", params={"symbol": "SHFE.rb2610"})
    assert tq.status_code == 200
    assert tq.json() == {
        "symbol": "SHFE.rb2610",
        "is_open": None,
        "raw_status": None,
        "reason": "not_received",
    }
    params = {"exchange_id": "SHFE", "product_id": "rb"}
    ctp = client.get("/ctp/fetch_trading_status", params=params)
    assert ctp.status_code == 200
    assert ctp.json() == {
        **params,
        "mode": "sandbox",
        "is_open": None,
        "raw_status": None,
        "reason": "not_received",
        "data": None,
    }
    factory.apis[0].callbacks.on_instrument_status(status_record())
    body = client.get("/ctp/fetch_trading_status", params=params).json()
    assert body["is_open"] is True and body["raw_status"] == "2"
    assert body["data"]["InstrumentID"] == "rb" and body["reason"] is None
    live = client.get("/ctp/fetch_trading_status", params={**params, "mode": "live"})
    assert live.json()["mode"] == "live" and live.json()["is_open"] is None


@pytest.mark.parametrize(
    ("provider", "params"),
    [
        ("tq", {}),
        ("tq", {"symbol": " "}),
        ("tq", {"symbol": "SHFE.rb2610", "mode": "sandbox"}),
        ("ctp", {"exchange_id": "SHFE"}),
        ("ctp", {"exchange_id": "SHFE", "product_id": "SHFE.rb2610"}),
        ("ctp", {"exchange_id": "SHFE", "product_id": "rb", "instrument_id": "rb2610"}),
        ("ctp", {"exchange_id": "SHFE", "product_id": "rb", "mode": "test"}),
    ],
)
def test_status_http_rejects_missing_invalid_or_undeclared_params(
    http, provider, params
):
    client, factory = http
    response = client.get(f"/{provider}/fetch_trading_status", params=params)
    assert response.status_code == 422
    assert factory.apis == []


def test_status_routes_require_bearer_auth(http):
    client, _ = http
    client.app.dependency_overrides.clear()
    for provider, params in [
        ("tq", {"symbol": "SHFE.rb2610"}),
        ("ctp", {"exchange_id": "SHFE", "product_id": "rb"}),
    ]:
        assert (
            client.get(f"/{provider}/fetch_trading_status", params=params).status_code
            == 401
        )


def test_tq_permission_denial_is_403_in_http(http, monkeypatch):
    client, _ = http

    def denied(request):
        raise HTTPException(403, "TQ_TRADING_STATUS_PERMISSION_DENIED")

    monkeypatch.setattr("src.router.tq_router.tq_manager.fetch_trading_status", denied)
    response = client.get("/tq/fetch_trading_status", params={"symbol": "SHFE.rb2610"})
    assert response.status_code == 403
    assert response.json()["detail"] == "TQ_TRADING_STATUS_PERMISSION_DENIED"


def test_status_openapi_exposes_nullable_types_parameters_and_original_ctp_fields():
    schema = main_app.openapi()
    for provider, fields in [
        ("tq", {"symbol"}),
        ("ctp", {"mode", "exchange_id", "product_id"}),
    ]:
        operation = schema["paths"][f"/{provider}/fetch_trading_status"]["get"]
        params = {field["name"]: field for field in operation["parameters"]}
        assert set(params) == fields
        assert all(field["description"] for field in params.values())
        assert operation["security"]
        reference = operation["responses"]["200"]["content"]["application/json"][
            "schema"
        ]["$ref"]
        model = schema["components"]["schemas"][reference.rsplit("/", 1)[1]]
        assert {part["type"] for part in model["properties"]["is_open"]["anyOf"]} == {
            "boolean",
            "null",
        }
        assert all(field["description"] for field in model["properties"].values())
        if provider == "ctp":
            assert params["mode"]["schema"]["default"] == "sandbox"
        else:
            assert "403" in operation["responses"]
    native = schema["components"]["schemas"]["CtpInstrumentStatus"]["properties"]
    assert {"InstrumentStatus", "EnterTime", "EnterReason", "TradingSegmentSN"} <= set(
        native
    )
