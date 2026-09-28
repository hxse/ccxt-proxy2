import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
import requests
import websockets

from src.tools.config_types import TqConfig
from src.tools.tq_client import TqClient
from src.tools.tq_network import tq_network

PROXY = "http://user:password@proxy.example:3128"
ENV_PROXY = "http://environment.example:8080"


@pytest.fixture
def captured_http(monkeypatch):
    calls = []
    monkeypatch.setenv("HTTP_PROXY", ENV_PROXY)
    monkeypatch.setenv("HTTPS_PROXY", ENV_PROXY)
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("no_proxy", raising=False)

    def send(session, request, **kwargs):
        calls.append((request, kwargs))
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"access_token":"access","refresh_token":"refresh"}'
        response.request = request
        response.url = request.url
        return response

    monkeypatch.setattr(requests.Session, "send", send)
    return calls


@pytest.mark.parametrize("proxy", [None, PROXY])
def test_real_sdk_authentication_uses_explicit_proxy(captured_http, proxy):
    with tq_network(proxy):
        from tqsdk import TqAuth

        auth = TqAuth("offline-user", "offline-password")
        assert auth._request_token({"grant_type": "password"}) == (
            "access",
            "refresh",
        )
    call = captured_http[-1]
    assert call[0].url.endswith("/protocol/openid-connect/token")
    assert call[0].method == "POST"
    assert call[1]["proxies"].get("https") == proxy
    assert call[1]["proxies"].get("http") == proxy


def test_nested_direct_context_exception_and_other_thread_do_not_leak_proxy(
    captured_http,
):
    barrier = threading.Barrier(2)

    def other_service():
        barrier.wait(timeout=3)
        requests.get("https://other.example/offline")

    with ThreadPoolExecutor(max_workers=1) as pool:
        with tq_network(PROXY):
            work = pool.submit(other_service)
            barrier.wait(timeout=3)
            requests.get("https://tq.example/outer")
            with pytest.raises(ValueError, match="offline"):
                with tq_network(None):
                    requests.get("https://tq.example/direct")
                    raise ValueError("offline")
            requests.get("https://tq.example/restored")
            work.result(timeout=3)
    requests.get("https://other.example/after")
    choices = {
        request.url: kwargs["proxies"].get("https")
        for request, kwargs in captured_http
    }
    assert choices == {
        "https://other.example/offline": ENV_PROXY,
        "https://tq.example/outer": PROXY,
        "https://tq.example/direct": None,
        "https://tq.example/restored": PROXY,
        "https://other.example/after": ENV_PROXY,
    }


def test_http_failure_restores_session_and_context(monkeypatch, captured_http):
    session = requests.Session()

    def fail(*args, **kwargs):
        raise requests.ConnectTimeout("offline")

    original = requests.Session.send
    with monkeypatch.context() as patch:
        patch.setattr(requests.Session, "send", fail)
        with pytest.raises(requests.ConnectTimeout):
            with tq_network(PROXY):
                session.get("https://tq.example/offline")
    assert requests.Session.send is original
    assert session.trust_env is True
    session.get("https://other.example/offline")
    assert captured_http[-1][1]["proxies"]["https"] == ENV_PROXY
    session.close()


def test_websocket_tasks_retain_choice_and_other_callers_keep_their_proxy():
    async def connection():
        await asyncio.sleep(0)
        return websockets.connect("wss://free-api.shinnytech.com/offline").proxy

    async def run():
        with tq_network(PROXY):
            proxied = asyncio.create_task(connection())
        with tq_network(None):
            direct = asyncio.create_task(connection())
        assert await proxied == PROXY
        assert await direct is None
        assert (
            websockets.connect("wss://other.example/offline", proxy=ENV_PROXY).proxy
            == ENV_PROXY
        )

    asyncio.run(run())


@pytest.mark.parametrize("proxy", [None, PROXY])
@pytest.mark.parametrize("connection_id", ["md", "ts"])
def test_real_sdk_websocket_passes_proxy_to_connection(
    monkeypatch, proxy, connection_id
):
    from tqsdk.connect import TqConnect
    from websockets.asyncio.client import connect

    calls = []

    class CapturedConnection(BaseException):
        pass

    async def capture(connection):
        calls.append(connection.proxy)
        raise CapturedConnection()

    monkeypatch.setattr(connect, "open_tcp_connection", capture)

    async def run():
        connector = TqConnect(logging.getLogger("offline.proxy"), connection_id)
        api = SimpleNamespace(_base_headers={})
        with tq_network(proxy):
            task = asyncio.create_task(
                connector._run(api, "wss://free-api.shinnytech.com/offline", None, None)
            )
        with pytest.raises(CapturedConnection):
            await task
        assert calls == [proxy]

    asyncio.run(run())


@pytest.mark.parametrize("enabled", [False, True])
def test_client_initialization_pump_and_close_apply_frozen_setting(
    tmp_path, monkeypatch, captured_http, enabled
):
    import src.tools.tq_trading_status as sdk

    expected = PROXY if enabled else None
    received = []

    def request(phase):
        requests.get(f"https://tq.example/{phase}")

    def create(**kwargs):
        received.append(kwargs["proxy_url"])
        request("initialize")
        return SimpleNamespace(
            has_trading_status_permission=lambda: True,
            wait_update=lambda **kw: request("pump"),
            close=lambda: request("close"),
        )

    monkeypatch.setattr(sdk, "TradingStatusTqApi", create)
    client = TqClient(
        TqConfig(username="offline", password="offline", enable_proxy=enabled),
        tmp_path / "tq.lock",
        proxy_url=PROXY,
    )
    try:
        client.initialize()
        client.pump()
    finally:
        client.close()
    assert received == [expected]
    assert [request.url.rsplit("/", 1)[1] for request, _ in captured_http] == [
        "initialize",
        "pump",
        "close",
    ]
    assert all(
        kwargs["proxies"].get("https") == expected for _, kwargs in captured_http
    )
