"""统一 FastAPI 的 CFB 鉴权、提交事实、诊断和原接口模型等价。"""

import json
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI

from src.cfb.errors import ServiceStatus
from src.cfb.ipc import Response
from src.cfb.manager import Manager
from src.cfb.results import OrderIdentity, SubmissionResult
from src.main import app as main_app
from src.router.auth_handler import manager
from src.router.cfb_router import cfb_router
from src.tools.config_types import CfbConfig
from Test.test_ctp_http import LocalClient

METHODS = {"create_market_order": "post", "create_limit_order": "post", "cancel_order": "post",
           "fetch_orders": "get", "fetch_trades": "get", "fetch_positions": "get", "fetch_balance": "get", "fetch_trading_status": "get"}


def normalized(value, document):
    if isinstance(value, dict):
        if "$ref" in value:
            name = value["$ref"].rsplit("/", 1)[-1]
            return normalized(document["components"]["schemas"][name], document)
        return {key: normalized(item, document) for key, item in value.items()
                if key not in {"title", "description", "examples", "example"}}
    if isinstance(value, list):
        return [normalized(item, document) for item in value]
    return value


def migrate_environment_schema(value):
    """来源样本保留原状；只允许已批准的环境输入迁移。"""
    if isinstance(value, list):
        return [migrate_environment_schema(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {key: migrate_environment_schema(item) for key, item in value.items()}
    if result.get("in") == "query" and result.get("name") == "mode":
        result.update(name="is_live", required=True, schema={"type": "boolean"})
    if "mode" in result.get("properties", {}):
        result["properties"].pop("mode")
        result["properties"]["is_live"] = {"type": "boolean"}
        result["required"] = ["is_live", *result.get("required", [])]
    return result


def test_original_eight_route_models_are_equivalent():
    before = json.loads(Path("Test/fixtures/cfb_source_contract.json").read_text())
    after = main_app.openapi()
    for name, method in METHODS.items():
        old = before["paths"]["/cfb/" + name][method]
        new = after["paths"]["/cfb/" + name][method]
        assert new["security"]
        assert migrate_environment_schema(normalized(old.get("parameters", []), before)) == normalized(new.get("parameters", []), after), name
        assert migrate_environment_schema(normalized(old.get("requestBody"), before)) == normalized(new.get("requestBody"), after), name
        code = "202" if method == "post" else "200"
        assert normalized(old["responses"][code], before) == normalized(new["responses"][code], after), name


def setup(monkeypatch, call):
    registry = Manager()
    registry.initialize(CfbConfig(), "sandbox")
    client = registry.get("sandbox")
    monkeypatch.setattr(client, "call", call)
    monkeypatch.setattr("src.router.cfb_router.cfb_manager", registry)
    app = FastAPI()
    app.state.service_runtime = SimpleNamespace(require=lambda service: None)
    app.include_router(cfb_router)
    return LocalClient(app)


def test_authentication_precedes_ipc(monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("未鉴权不应进入 IPC")
    http = setup(monkeypatch, forbidden)
    result = http.get('/cfb/fetch_balance?is_live=false')
    assert result.status_code == 401


def test_failure_keeps_submission_identity_and_replayed_request_id(monkeypatch):
    identity = OrderIdentity(exchange_id="DCE", instrument_id="m2701", trading_day="20260929",
                             front_id=3, session_id=-123, order_ref="000018")
    payload = SubmissionResult(request_id="cfb-original", submission_status="submitted", order_id="000123", identity=identity).model_dump(mode="json")
    payload["error"] = {"code": "GUI_RESET_FAILED", "message": "收尾失败"}
    calls = []

    async def call(kind, **kwargs):
        calls.append((kind, kwargs))
        return Response(status=503, body=payload, headers={"X-Request-ID": "cfb-original", "Retry-After": "600"})

    http = setup(monkeypatch, call)
    http.app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    result = http.post("/cfb/create_limit_order", headers={"Idempotency-Key": "one"}, json={"is_live": False, 
        "exchange_id": "DCE", "instrument_id": "m2701", "side": "buy", "offset": "open", "volume": 1, "price": 3514.25})
    assert result.status_code == 503 and result.json() == payload
    assert result.headers["X-Request-ID"] == "cfb-original" and result.headers["Retry-After"] == "600"
    assert result.headers["Cache-Control"] == "no-store" and len(calls) == 1
    assert calls[0][1]["operation"].parameters["price"] == 3514.25
    assert calls[0][1]["key"] == "one"


def test_diagnostics_preserve_terminal_readiness_separately(monkeypatch):
    async def call(kind, **kwargs):
        return Response(status=200, body=ServiceStatus(trading_ready=False).model_dump(mode="json"))
    http = setup(monkeypatch, call)
    http.app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    assert http.get('/cfb/status?is_live=false').status_code == 200
    assert http.get('/cfb/readyz?is_live=false').status_code == 503


def test_configuration_mismatch_cannot_report_ready(tmp_path, monkeypatch):
    from src.cfb.errors import BridgeError
    from src.cfb.server import response
    from Test.test_cfb_ipc import fixture

    service, dispatcher, server, client = fixture(tmp_path)
    client.settings = client.settings.model_copy(update={"vnc_enabled": False})
    assert client.settings.identity_signature != service.settings.identity_signature
    assert service.status().trading_ready is True

    async def through_server(kind, **kwargs):
        import asyncio

        from src.cfb.ipc import Request
        from src.cfb.results import Reply

        request = Request(kind=kind, request_id=kwargs["request_id"],
                          operation=kwargs.get("operation"),
                          configuration=client.settings.identity_signature)
        disconnected = asyncio.create_task(asyncio.Event().wait())
        try:
            return await server.handle(request, disconnected)
        except BridgeError as exc:
            return response(service, Reply(request_id=request.request_id, status=exc.status,
                body=exc.response(request.request_id).model_dump(mode="json")))
        finally:
            disconnected.cancel()
            await asyncio.gather(disconnected, return_exceptions=True)

    http = setup(monkeypatch, through_server)
    http.app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    status = http.get('/cfb/status?is_live=false')
    assert status.status_code == 200 and status.json()["trading_ready"] is True
    for path in ('/cfb/readyz?is_live=false', '/cfb/fetch_positions?is_live=false'):
        result = http.get(path)
        assert result.status_code == 503
        assert result.json()["error"]["code"] == "SERVICE_NOT_READY"
        assert "配置与执行器不一致" in result.json()["error"]["message"]
    assert not dispatcher.queue
    client.settings = service.settings
    assert http.get('/cfb/readyz?is_live=false').status_code == 200
    dispatcher.state.trading_ready = False
    assert http.get('/cfb/readyz?is_live=false').status_code == 503
