"""离线转发契约：不连接 CFB，不登录或发送真实委托。"""

import asyncio
import gzip

import httpx
import pytest
from fastapi import FastAPI

from src.cfb_contract import CFB_ROUTES
from src.router.auth_handler import manager
from src.router.cfb_router import cfb_router
from src.tools.cfb_proxy import CfbProxy
from src.tools.config_types import AppConfig
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import add_request_context
from Test.test_ctp_http import LocalClient


@pytest.fixture
def http(monkeypatch):
    config = AppConfig.model_validate(
        {
            "SECRET": "offline",
            "cfb": {"base_url": "http://cfb.invalid:45173"},
            "service_whitelist": [{"service": "cfb"}],
        }
    )
    runtime = ServiceRuntime(config)
    proxy = CfbProxy()
    calls = []

    def handler(request):
        return httpx.Response(200, json={"upstream": True})

    async def upstream(request):
        calls.append(request)
        result = handler(request)
        return await result if asyncio.iscoroutine(result) else result

    created = []

    def create_client(**kwargs):
        client = httpx.AsyncClient(transport=httpx.MockTransport(upstream), **kwargs)
        created.append(client)
        return client

    def configure(replacement):
        nonlocal handler
        handler = replacement

    monkeypatch.setattr("src.tools.cfb_proxy.AsyncClient", create_client)
    monkeypatch.setattr("src.router.cfb_router.cfb_proxy", proxy)
    app = FastAPI()
    app.state.service_runtime = runtime
    app.include_router(cfb_router)
    app.middleware("http")(add_request_context)
    app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    runtime.start(None, None, None, proxy)
    assert runtime.initialized == ["cfb"] and len(created) == 1
    try:
        yield LocalClient(app), configure, calls, proxy, runtime
    finally:
        runtime.close()
        asyncio.run(proxy.close())
        assert len(created) == 1 and created[0].is_closed


@pytest.mark.parametrize("mode", ["sandbox", "live"])
@pytest.mark.parametrize(("path", "method"), CFB_ROUTES.items())
def test_all_routes_forward_modes_and_future_fields_without_business_validation(
    http, path, method, mode
):
    client, configure, calls, _, _ = http
    status = 202 if method == "post" else 200
    body = b'{"request_id":"cfb-original", "new_upstream_field": [1,null,"value"]}\n'
    configure(
        lambda request: httpx.Response(
            status,
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Request-ID": "cfb-original",
                "Cache-Control": "no-store",
                "X-Upstream-Extra": "preserved",
            },
        )
    )
    payload = (
        '{ "mode":"' + mode + '", "future_field": {"x":1}, "price": 123.4500 }'
    ).encode()
    query = f"mode={mode}&new_field=a%2Bb&new_field=c%20d&order_sys_id=%20%2042"
    kwargs = (
        {
            "content": payload,
            "headers": {
                "Content-Type": "application/json",
                "Idempotency-Key": "original-key",
                "Authorization": "Bearer proxy-only",
                "Cookie": "private=proxy-only",
            },
        }
        if method == "post"
        else {}
    )
    response = client.request(method.upper(), path + "?" + query, **kwargs)
    assert response.status_code == status and response.content == body
    assert response.headers["x-request-id"] == response.json()["request_id"]
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-upstream-extra"] == "preserved"
    assert len(calls) == 1
    request = calls[0]
    assert request.method == method.upper()
    assert request.url.host == "cfb.invalid" and request.url.port == 45173
    assert request.url.path == path and request.url.query == query.encode()
    assert "authorization" not in request.headers and "cookie" not in request.headers
    if method == "post":
        assert request.content == payload
        assert request.headers["idempotency-key"] == "original-key"


@pytest.mark.parametrize("status", [202, 307, 409, 422, 429, 500, 501, 502, 503, 504])
def test_upstream_status_and_body_are_returned_once_without_redirect_or_retry(
    http, status
):
    client, configure, calls, _, _ = http
    content = (
        b'{ "request_id":"cfb-original", "submission_status":"submitted",'
        b' "order_id":null, "identity":{"exchange_id":"DCE",'
        b' "instrument_id":"m2701", "trading_day":"20260924",'
        b' "front_id":3, "session_id":-123, "order_ref":"000018"},'
        b' "execution":{"kind":"limit", "price":3500.0, "time_in_force":"GFD"},'
        b' "verification":{"status":"pending", "correlation":"order_ref"}'
    )
    if status >= 400:
        content += (
            b', "error":{"code":"UPSTREAM", "message":"upstream failed",'
            b' "extra":true}'
        )
    content += b"}"
    configure(
        lambda request: httpx.Response(
            status,
            content=content,
            headers={
                "Content-Type": "application/json",
                "Retry-After": "10",
                "Location": "http://another.invalid/do-not-follow",
            },
        )
    )
    response = client.post("/cfb/create_limit_order", content=b'{"mode":"live"}')
    assert response.status_code == status and response.content == content
    assert response.headers["retry-after"] == "10"
    assert response.headers["location"] == "http://another.invalid/do-not-follow"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "identity_query",
    [
        "order_sys_id=%20%20648294",
        "trading_day=20260924&front_id=3&session_id=-123&order_ref=000018",
    ],
)
def test_order_followup_preserves_exact_identifiers(http, identity_query):
    client, configure, calls, _, _ = http
    identity = (
        {
            "exchange_id": "DCE",
            "instrument_id": "m2701",
            "trading_day": "20260924",
            "front_id": 3,
            "session_id": -123,
            "order_ref": "000018",
        }
        if identity_query.startswith("trading_day=")
        else None
    )
    payload = {
        "request_id": "cfb-query",
        "observed_at": "2026-09-24T09:30:00+08:00",
        "source": "terminal_csv_and_native" if identity else "terminal_csv",
        "consistency": "changing",
        "orders": [],
        "identity": identity,
    }
    configure(lambda request: httpx.Response(200, json=payload))
    query = "mode=sandbox&exchange_id=DCE&instrument_id=m2701&" + identity_query

    response = client.get("/cfb/fetch_orders?" + query)

    assert response.status_code == 200 and response.json() == payload
    assert len(calls) == 1 and calls[0].url.query == query.encode()


def test_proxy_does_not_parse_json_and_preserves_duplicate_idempotency_headers(http):
    client, configure, calls, _, _ = http
    configure(lambda request: httpx.Response(422, text="upstream validation error"))
    response = client.post(
        "/cfb/cancel_order",
        content=b"not-json",
        headers=[
            ("Content-Type", "text/plain"),
            ("Idempotency-Key", "first"),
            ("Idempotency-Key", "second"),
        ],
    )
    assert response.status_code == 422 and response.text == "upstream validation error"
    assert calls[0].content == b"not-json"
    assert calls[0].headers.get_list("idempotency-key") == ["first", "second"]


def test_compressed_response_has_correct_http_headers_after_decompression(http):
    client, configure, _, _, _ = http
    payload = b'{"extra":"unchanged"}'
    configure(
        lambda request: httpx.Response(
            200,
            content=gzip.compress(payload),
            headers={
                "Content-Encoding": "gzip",
                "Content-Type": "application/json",
                "Connection": "keep-alive, x-internal",
                "X-Internal": "connection-only",
            },
        )
    )
    response = client.get("/cfb/fetch_balance")
    assert response.content == payload and int(
        response.headers["content-length"]
    ) == len(payload)
    for header in ("content-encoding", "connection", "x-internal"):
        assert header not in response.headers


@pytest.mark.parametrize(
    ("error", "status", "code"),
    [
        (httpx.ConnectError, 502, "CFB_PROXY_NETWORK_ERROR"),
        (httpx.ReadError, 502, "CFB_PROXY_NETWORK_ERROR"),
        (httpx.ReadTimeout, 504, "CFB_PROXY_TIMEOUT"),
    ],
)
def test_network_failures_have_explicit_proxy_errors_and_no_retry(
    http, error, status, code
):
    client, configure, calls, _, _ = http

    def fail(request):
        raise error("private upstream detail", request=request)

    configure(fail)
    response = client.post("/cfb/create_market_order", json={"mode": "live"})
    assert response.status_code == status and response.json() == {
        "detail": {"code": code}
    }
    assert len(calls) == 1


def test_total_timeout_cancels_waiting_and_does_not_retry(http):
    client, configure, calls, proxy, _ = http
    proxy._timeout = 0.01
    cancelled = []

    async def wait_forever(request):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    configure(wait_forever)
    response = client.post("/cfb/create_limit_order", json={"mode": "live"})
    assert response.status_code == 504
    assert response.json() == {"detail": {"code": "CFB_PROXY_TIMEOUT"}}
    assert len(calls) == 1 and cancelled == [True]


def test_auth_and_whitelist_are_checked_before_forwarding(http):
    client, _, calls, _, runtime = http
    client.app.dependency_overrides.clear()
    assert client.get("/cfb/fetch_balance").status_code == 401
    client.app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    runtime.close()
    assert client.get("/cfb/fetch_balance").json() == {
        "detail": {"code": "SERVICE_NOT_READY", "service": "cfb"}
    }
    client.app.state.service_runtime = ServiceRuntime(AppConfig(SECRET="offline"))
    assert client.get("/cfb/fetch_balance").json() == {
        "detail": {"code": "SERVICE_NOT_ENABLED", "service": "cfb"}
    }
    assert calls == []


def test_only_fixed_business_paths_are_exposed_and_reads_are_not_cached(http):
    client, _, calls, _, _ = http
    assert client.get("/cfb/v1/status").status_code == 404
    assert client.post("/cfb/fetch_balance").status_code == 405
    client.get("/cfb/fetch_balance")
    client.get("/cfb/fetch_balance")
    assert len(calls) == 2
