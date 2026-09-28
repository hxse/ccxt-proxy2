"""公共时间薄转发的离线 HTTP 测试，不请求真实币安接口。"""

import asyncio

import aiohttp
import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.main import app as main_app
from src.router.auth_handler import manager as auth_manager
from src.router.system_router import system_router
from src.tools import public_time
from src.tools.config_loader import load_config
from src.tools.config_types import AppConfig
from Test.helpers.public_time_transport import TimeSession
from Test.test_ctp_http import LocalClient


@pytest.fixture
def http(monkeypatch):
    app = FastAPI()
    app.include_router(system_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    created = []
    calls = []
    monkeypatch.setattr(public_time, "config", AppConfig(SECRET="offline"))

    async def no_market_loading(*args, **kwargs):
        pytest.fail("公共取时不得加载市场或依赖交易实例")

    monkeypatch.setattr(public_time._TimeBinance, "load_markets", no_market_loading)

    def configure(handler):
        def create_client(**kwargs):
            client = TimeSession(handler, calls, **kwargs)
            created.append(client)
            return client

        monkeypatch.setattr(public_time.aiohttp, "ClientSession", create_client)

    configure(lambda request: httpx.Response(200, json={"serverTime": 1234567890000}))
    yield LocalClient(app), configure, calls
    assert all(client.closed and not client.trust_env for client in created)


def test_time_is_fetched_for_each_request_and_upstream_fields_are_preserved(http):
    client, configure, calls = http
    timestamps = iter([1234567890000, 1234567890123])
    configure(
        lambda request: httpx.Response(
            200, json={"serverTime": next(timestamps), "extra": "upstream field"}
        )
    )
    first = client.get("/system/fetch_time", headers={"Authorization": "Bearer local"})
    second = client.get("/system/fetch_time")
    assert first.status_code == second.status_code == 200
    assert first.json() == {"serverTime": 1234567890000, "extra": "upstream field"}
    assert second.json()["serverTime"] == 1234567890123
    assert (
        first.headers["cache-control"] == second.headers["cache-control"] == "no-store"
    )
    assert len(calls) == 2
    for request in calls:
        assert request.method == "GET"
        assert str(request.url) == "https://api.binance.com/api/v3/time"
        assert not request.content
        assert "authorization" not in request.headers
        assert "x-mbx-apikey" not in request.headers
        assert request.headers["cache-control"] == "no-cache"


def test_time_requires_local_auth_before_contacting_upstream(http):
    client, _, calls = http
    client.app.dependency_overrides.clear()
    assert client.get("/system/fetch_time").status_code == 401
    assert calls == []


@pytest.mark.parametrize("params", [{"mode": "sandbox"}, {"url": "http://other"}])
def test_time_rejects_all_query_parameters_before_contacting_upstream(http, params):
    client, _, calls = http
    response = client.get("/system/fetch_time", params=params)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", next(iter(params))]
    assert calls == []


@pytest.mark.parametrize("status", [302, 418, 429, 451, 503])
def test_time_exposes_upstream_error_status_without_retry_or_body_leak(http, status):
    client, configure, calls = http
    configure(
        lambda request: httpx.Response(
            status,
            headers={"Location": "https://other.example/time"},
            text="upstream internal details",
        )
    )
    response = client.get("/system/fetch_time")
    assert response.status_code == 502
    assert response.json() == {
        "detail": {"code": "PUBLIC_TIME_UPSTREAM_ERROR", "upstream_status": status}
    }
    assert len(calls) == 1


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"serverTime": True},
        {"serverTime": "123"},
        {"serverTime": 1.5},
        {"serverTime": -1},
        {"serverTime": 2**63},
    ],
)
def test_time_rejects_invalid_payload_instead_of_substituting_local_time(http, payload):
    client, configure, calls = http
    configure(lambda request: httpx.Response(200, json=payload))
    response = client.get("/system/fetch_time")
    assert response.status_code == 502
    assert response.json() == {"detail": {"code": "PUBLIC_TIME_INVALID_RESPONSE"}}
    assert len(calls) == 1


def test_time_rejects_non_json_response(http):
    client, configure, _ = http
    configure(lambda request: httpx.Response(200, text="<html>upstream page</html>"))
    response = client.get("/system/fetch_time")
    assert response.status_code == 502
    assert response.json() == {"detail": {"code": "PUBLIC_TIME_INVALID_RESPONSE"}}


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (aiohttp.ConnectionTimeoutError, 504, "PUBLIC_TIME_TIMEOUT"),
        (aiohttp.SocketTimeoutError, 504, "PUBLIC_TIME_TIMEOUT"),
        (aiohttp.ClientConnectionError, 502, "PUBLIC_TIME_NETWORK_ERROR"),
    ],
)
def test_time_network_errors_are_explicit_and_not_retried(http, error, status, code):
    client, configure, calls = http

    def fail(request):
        raise error("upstream internal details")

    configure(fail)
    response = client.get("/system/fetch_time")
    assert response.status_code == status
    assert response.json() == {"detail": {"code": code}}
    assert len(calls) == 1


def test_time_has_total_deadline_even_if_transport_does_not_time_out(http, monkeypatch):
    client, configure, calls = http
    cancelled = []

    async def wait_forever(request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    monkeypatch.setattr(public_time, "PUBLIC_TIME_TIMEOUT_SECONDS", 0.01)
    configure(wait_forever)
    response = client.get("/system/fetch_time")
    assert response.status_code == 504
    assert response.json() == {"detail": {"code": "PUBLIC_TIME_TIMEOUT"}}
    assert len(calls) == 1 and cancelled == [True]


def test_time_openapi_explains_units_auth_and_errors_without_parameters():
    schema = main_app.openapi()
    operation = schema["paths"]["/system/fetch_time"]["get"]
    assert operation["security"]
    assert operation.get("parameters", []) == []
    assert "requestBody" not in operation
    assert {"200", "401", "422", "502", "504"} <= operation["responses"].keys()
    field = schema["components"]["schemas"]["PublicTimeResponse"]["properties"][
        "serverTime"
    ]
    assert field["type"] == "integer" and field["format"] == "int64"
    assert "毫秒" in field["description"]


@pytest.mark.parametrize(
    "profile,uses_proxy", [("dev", False), ("local", False), ("remote", True)]
)
def test_time_uses_scene_proxy_without_credentials_or_enabled_trading(
    http, tmp_path, monkeypatch, profile, uses_proxy
):
    client, _, calls = http
    path = tmp_path / "config.toml"
    path.write_text("""
SECRET = 'offline'
[proxy]
http = 'http://proxy.example:8888'
[binance]
enable_proxy = false
[overrides.remote.binance]
enable_proxy = true
""")
    monkeypatch.setenv("HTTPS_PROXY", "http://unrelated.example:9999")
    selected = load_config(path, profile=profile)
    assert selected.service_whitelist == []
    assert selected.binance is not None
    assert selected.binance.live is None and selected.binance.test is None
    monkeypatch.setattr(public_time, "config", selected)
    response = client.get("/system/fetch_time")
    assert response.status_code == 200
    assert len(calls) == 1
    assert calls[0].extensions["proxy"] == (
        "http://proxy.example:8888" if uses_proxy else None
    )
    assert calls[0].extensions["timeout"].total == 5
    assert str(calls[0].url) == "https://api.binance.com/api/v3/time"


def test_time_proxy_requires_address_even_without_trading_whitelist():
    with pytest.raises(ValidationError, match="binance.enable_proxy"):
        AppConfig(SECRET="offline", binance={"enable_proxy": True})


def test_time_really_uses_ccxt_fetch_time(http, monkeypatch):
    client, _, calls = http
    original = public_time.ccxt.binance.fetch_time
    invoked = []

    async def observe(exchange, params=None):
        invoked.append(exchange)
        return await original(exchange, params or {})

    monkeypatch.setattr(public_time.ccxt.binance, "fetch_time", observe)
    assert client.get("/system/fetch_time").status_code == 200
    assert len(invoked) == len(calls) == 1
    assert invoked[0].session is None


def test_time_cancellation_closes_session_without_becoming_a_http_error(http):
    _, configure, calls = http

    async def cancel(request):
        raise asyncio.CancelledError

    configure(cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(public_time.fetch_public_time())
    assert len(calls) == 1
