"""公共时间薄转发的离线 HTTP 测试，不请求真实币安接口。"""

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from src.main import app as main_app
from src.router.auth_handler import manager as auth_manager
from src.router.system_router import system_router
from src.tools import public_time
from Test.test_ctp_http import LocalClient


@pytest.fixture
def http(monkeypatch):
    app = FastAPI()
    app.include_router(system_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    created = []
    calls = []

    def configure(handler):
        async def upstream(request):
            calls.append(request)
            result = handler(request)
            return await result if asyncio.iscoroutine(result) else result

        def create_client(**kwargs):
            client = httpx.AsyncClient(
                transport=httpx.MockTransport(upstream), **kwargs
            )
            created.append(client)
            return client

        monkeypatch.setattr(public_time, "AsyncClient", create_client)

    configure(lambda request: httpx.Response(200, json={"serverTime": 1234567890000}))
    yield LocalClient(app), configure, calls
    assert all(client.is_closed for client in created)


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
        (httpx.ConnectTimeout, 504, "PUBLIC_TIME_TIMEOUT"),
        (httpx.ReadTimeout, 504, "PUBLIC_TIME_TIMEOUT"),
        (httpx.ConnectError, 502, "PUBLIC_TIME_NETWORK_ERROR"),
    ],
)
def test_time_network_errors_are_explicit_and_not_retried(http, error, status, code):
    client, configure, calls = http

    def fail(request):
        raise error("upstream internal details", request=request)

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
