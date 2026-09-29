"""显式环境选择的 HTTP、CLI 和历史幂等边界；不访问真实交易服务。"""

import hashlib
import json
import sqlite3

import pytest
from fastapi import FastAPI

from src.cfb.config import BridgeConfig, Settings
from src.cfb.journal import Journal
from src.cfb.models import Operation
from src.cfb.results import Reply
from src.main import app as main_app
from src.router.auth_handler import manager
from src.router.cache_router import cache_router
from src.router.cfb_router import cfb_router
from src.router.ctp_router import ctp_router
from src.router.trader_router import ccxt_router
from Test.test_ctp_http import LocalClient


@pytest.fixture
def http(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("非法环境不得进入服务或数据库")

    monkeypatch.setattr("src.router.trader_router.exchange_manager.get_client", forbidden)
    monkeypatch.setattr("src.router.ctp_router.ctp_manager.get_client", forbidden)
    monkeypatch.setattr("src.router.cfb_router.cfb_manager.get", forbidden)
    monkeypatch.setattr("src.router.cache_router._cache", forbidden)
    app = FastAPI()
    for router in (ccxt_router, ctp_router, cfb_router, cache_router):
        app.include_router(router)
    app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    return LocalClient(app)


@pytest.mark.parametrize("path,base", [
    ("/ccxt/fetch_balance", {"exchange_name": "binance", "market": "future"}),
    ("/ctp/fetch_balance", {}),
    ("/cfb/fetch_balance", {}),
    ("/cfb/status", {}),
    ("/cache/summary", {}),
])
@pytest.mark.parametrize("selection", [
    {}, {"mode": "live"}, {"is_live": "true", "mode": "sandbox"},
    {"is_live": "false", "trading_env": "live"},
    *({"is_live": value} for value in ("", "0", "1", "yes", "no", "True", "live", "null")),
])
def test_query_rejects_missing_ambiguous_or_legacy_environment(http, path, base, selection):
    result = http.get(path, params=base | selection)
    assert result.status_code == 422, result.text
    if path.startswith("/cfb/"):
        assert result.json()["error"]["code"] == "INVALID_ARGUMENTS"
        problems = result.json()["error"]["details"]
    else:
        problems = result.json()["detail"]
    assert any("is_live" in str(item) or "mode" in str(item) or "trading_env" in str(item) for item in problems)


@pytest.mark.parametrize("path,base", [
    ("/ccxt/create_market_order", {"exchange_name": "binance", "market": "future",
                                   "symbol": "BTC/USDT:USDT", "side": "buy", "amount": 1}),
    ("/ctp/create_market_order", {"exchange_id": "SHFE", "instrument_id": "rb2610",
                                  "side": "buy", "offset": "open", "volume": 1}),
    ("/cfb/create_market_order", {"exchange_id": "SHFE", "instrument_id": "rb2610",
                                  "side": "buy", "offset": "open", "volume": 1}),
])
@pytest.mark.parametrize("selection", [
    {}, {"mode": "sandbox"}, {"is_live": False, "mode": "live"},
    {"is_live": True, "trading_env": "sandbox"},
    *({"is_live": value} for value in (None, 0, 1, "false", "true", "sandbox")),
])
def test_json_requires_real_boolean_before_any_execution(http, path, base, selection):
    result = http.post(path, json=base | selection)
    assert result.status_code == 422, result.text
    problems = result.json().get("detail") or result.json()["error"]["details"]
    assert any("is_live" in str(item) or "mode" in str(item) or "trading_env" in str(item) for item in problems)


def test_all_environment_routes_publish_required_boolean_without_default():
    schema = main_app.openapi()

    def models(body):
        if "$ref" in body:
            yield schema["components"]["schemas"][body["$ref"].rsplit("/", 1)[1]]
        for child in body.get("oneOf", []):
            yield from models(child)

    for path, methods in schema["paths"].items():
        selected = path.startswith(("/ccxt/", "/ctp/", "/cfb/")) or path == "/cache/summary"
        for method, operation in methods.items():
            if method == "get":
                params = {p["name"]: p for p in operation.get("parameters", [])}
                if not selected:
                    assert "is_live" not in params
                    continue
                assert "mode" not in params and "trading_env" not in params
                assert params["is_live"]["required"] is True
                assert params["is_live"]["schema"]["type"] == "boolean"
                assert "default" not in params["is_live"]["schema"]
            elif method == "post" and selected:
                body = operation["requestBody"]["content"]["application/json"]["schema"]
                variants = list(models(body))
                assert variants
                for model in variants:
                    assert "is_live" in model["required"]
                    assert model["properties"]["is_live"]["type"] == "boolean"
                    assert "default" not in model["properties"]["is_live"]
                    assert "mode" not in model["properties"]


def test_existing_journal_fingerprint_and_unknown_submission_survive_rename(tmp_path):
    # 迁移前 LimitOrder 的完整规范化 JSON，不调用新序列化器生成预期。
    old_parameters = {"mode": "sandbox", "exchange_id": "DCE", "instrument_id": "m2701",
                      "side": "buy", "offset": "open", "volume": 1, "price": 3514.25,
                      "hedge_flag": "speculation", "invest_unit_id": "", "time_in_force": "GFD"}
    old_canonical = {"action": "create_limit_order", "parameters": old_parameters}
    old_fingerprint = hashlib.sha256(json.dumps(old_canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    parameters = {key: value for key, value in old_parameters.items() if key != "mode"}
    operation = Operation(action="create_limit_order", parameters=parameters | {"is_live": False})
    assert Journal.fingerprint(operation) == old_fingerprint
    journal = Journal(Settings(bridge=BridgeConfig(data_dir=tmp_path)))
    assert journal.admit("old-request", operation, "persistent-key") is None
    old = Reply(request_id="old-request", status=502,
                body={"request_id": "old-request", "submission_status": "unknown", "order_id": "000123"})
    journal.finish(old)
    with sqlite3.connect(journal.path) as db:
        db.execute("UPDATE operations SET fingerprint=?", (old_fingerprint,))
    assert journal.admit("must-not-resubmit", operation, "persistent-key") == old
    changed = Operation(action=operation.action, parameters=operation.parameters | {"volume": 2})
    conflict = journal.lookup(changed, "persistent-key")
    assert conflict is not None and conflict.status == 409
    error = conflict.body["error"]
    assert isinstance(error, dict) and error["code"] == "IDEMPOTENCY_CONFLICT"
