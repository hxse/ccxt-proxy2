"""真实 HTTP 与 SDK 所有权链的故障隔离，不访问任何真实上游。"""

import asyncio
import threading
import time
from contextlib import contextmanager

import ccxt
import httpx
import pytest

from src.main import app
from src.router.auth_handler import manager as auth_manager
from src.tools.cfb_proxy import CfbProxy
from src.tools.config_types import AppConfig
from src.tools.exchange_manager import ExchangeManager
from src.tools.service_runtime import ServiceRuntime
from src.tools.tq_manager import TqManager
from Test.test_ctp_http import LocalClient
from Test.test_tq_lifecycle import OwnedApi

IDENTITIES = {
    "binance": "ccxt/binance/future/live",
    "kraken": "ccxt/kraken/future/live",
    "tq": "tq",
    "cfb": "cfb",
}


def request_service(client, provider):
    if provider == "tq":
        return client.get("/tq/fetch_tick", params={"symbol": "SHFE.rb2610"})
    if provider == "cfb":
        return client.get("/cfb/fetch_balance", params={"mode": "live"})
    return client.get(
        "/ccxt/fetch_balance",
        params={"exchange_name": provider, "market": "future", "mode": "live"},
    )


def wait_for_states(client, expected):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get("/readyz")
        assert response.status_code == 200
        if response.json()["services"] == expected:
            return response.json()
        threading.Event().wait(0.005)
    pytest.fail(f"unexpected service states: {response.json()}")


@pytest.fixture
def environment(tmp_path, monkeypatch):
    @contextmanager
    def install(*, fail=None, block=None):
        config = AppConfig.model_validate(
            {
                "SECRET": "offline-service-isolation-secret",
                "ohlcv_cache": {"database_path": str(tmp_path / "cache.duckdb")},
                "binance": {"live": {"api_key": "offline", "secret": "offline"}},
                "kraken": {"live": {"api_key": "offline", "secret": "offline"}},
                "tq": {"username": "offline", "password": "offline"},
                "cfb": {"base_url": "http://cfb.invalid"},
                "service_whitelist": [
                    {"service": "tq"},
                    {"service": "cfb"},
                    {
                        "service": "ccxt",
                        "exchange": "binance",
                        "market": "future",
                        "mode": "live",
                    },
                    {
                        "service": "ccxt",
                        "exchange": "kraken",
                        "market": "future",
                        "mode": "live",
                    },
                ],
            }
        )
        runtime = ServiceRuntime(config)
        release, entered = threading.Event(), threading.Event()
        attempted = []

        def initialize(provider):
            attempted.append(provider)
            if provider == block:
                entered.set()
                assert release.wait(5)
            if fail in {provider, "all"}:
                raise RuntimeError("private-upstream-sentinel")

        class RawExchange:
            has = {"fetchBalance": True}

            def __init__(self, provider):
                self.provider = provider
                self.closed = 0
                self.error = None

            def load_markets(self):
                initialize(self.provider)

            def fetch_balance(self, params):
                if self.error is not None:
                    raise self.error
                return {"free": {}, "used": {}, "total": {"USD": 1.0}}

            def close(self):
                self.closed += 1

        class Registry(ExchangeManager):
            _instance = None

        registry = Registry()
        raw = {name: RawExchange(name) for name in ("binance", "kraken")}
        monkeypatch.setattr(
            "src.tools.exchange_manager.get_binance_exchange",
            lambda *args: raw["binance"],
        )
        monkeypatch.setattr(
            "src.tools.exchange_manager.get_kraken_exchange",
            lambda *args: raw["kraken"],
        )
        tq = TqManager(config.tq, tmp_path / "tq.lock", access_guard=runtime.require)
        apis = []

        def create_api(_):
            initialize("tq")
            api = OwnedApi()
            apis.append(api)
            return api

        monkeypatch.setattr(tq._client, "_create_api", create_api)
        proxy = CfbProxy()
        original_initialize = proxy.initialize

        def initialize_cfb(settings):
            initialize("cfb")
            original_initialize(settings)

        monkeypatch.setattr(proxy, "initialize", initialize_cfb)
        monkeypatch.setattr(
            "src.tools.cfb_proxy.AsyncClient",
            lambda **kwargs: httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json={"upstream": True})
                ),
                **kwargs,
            ),
        )
        monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
        monkeypatch.setattr(app.state, "service_runtime", runtime)
        monkeypatch.setattr("src.tools.shared.exchange_manager", registry)
        monkeypatch.setattr("src.router.trader_router.exchange_manager", registry)
        monkeypatch.setattr("src.tools.tq_manager.tq_manager", tq)
        monkeypatch.setattr("src.router.tq_router.tq_manager", tq)
        monkeypatch.setattr("src.tools.cfb_proxy.cfb_proxy", proxy)
        monkeypatch.setattr("src.router.cfb_router.cfb_proxy", proxy)
        monkeypatch.setattr(
            "src.tools.telegram_manager.telegram_manager.close", lambda: None
        )
        monkeypatch.setitem(
            app.dependency_overrides, auth_manager, lambda: {"sub": "offline"}
        )
        with asyncio.Runner() as runner:
            lifecycle = app.router.lifespan_context(app)
            runner.run(lifecycle.__aenter__())
            try:
                yield LocalClient(app), runtime, entered, release, attempted, raw, apis
            finally:
                release.set()
                runner.run(lifecycle.__aexit__(None, None, None))
        assert all(api.closed.is_set() for api in apis)
        assert proxy._client is None

    return install


@pytest.mark.parametrize("failed", ["tq", "cfb", "binance", "kraken", "all"])
def test_failed_sdk_does_not_block_other_http_routes(environment, failed):
    with environment(fail=failed) as (client, runtime, _, _, attempted, raw, _):
        expected = {
            identity: "failed" if failed in {name, "all"} else "ready"
            for name, identity in IDENTITIES.items()
        }
        state = wait_for_states(client, expected)
        assert state["initialized"] == [
            key for key in state["services"] if expected[key] == "ready"
        ]
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/openapi.json").status_code == 200
        for name, identity in IDENTITIES.items():
            response = request_service(client, name)
            if expected[identity] == "failed":
                assert response.status_code == 503
                assert response.json() == {
                    "detail": {"code": "SERVICE_NOT_READY", "service": identity}
                }
                assert "private-upstream-sentinel" not in response.text
            else:
                assert response.status_code == 200, response.text
        assert sorted(attempted) == sorted(IDENTITIES)
        for name, exchange in raw.items():
            assert exchange.closed == int(expected[IDENTITIES[name]] == "failed")
        assert runtime.ready
    assert all(exchange.closed == 1 for exchange in raw.values())


@pytest.mark.parametrize("blocked", ["tq", "cfb", "binance", "kraken"])
def test_blocked_initialization_does_not_delay_healthy_services(environment, blocked):
    with environment(block=blocked) as (client, _, entered, release, _, _, _):
        assert entered.wait(2)
        expected = {
            identity: "initializing" if name == blocked else "ready"
            for name, identity in IDENTITIES.items()
        }
        wait_for_states(client, expected)
        # 阻塞事件尚未放行，健康请求已经能成功；故障入口不能排队等待初始化。
        for name in IDENTITIES:
            response = request_service(client, name)
            assert response.status_code == (503 if name == blocked else 200), (
                response.text
            )
        assert not release.is_set()
        release.set()
        wait_for_states(client, dict.fromkeys(IDENTITIES.values(), "ready"))
        assert request_service(client, blocked).status_code == 200


def test_tq_worker_exit_invalidates_routes_without_affecting_other_services(
    environment,
):
    with environment() as (client, _, _, _, _, _, apis):
        wait_for_states(client, dict.fromkeys(IDENTITIES.values(), "ready"))
        apis[0].fail_pump = True
        assert apis[0].closed.wait(2)
        response = client.get(
            "/tq/fetch_trading_status", params={"symbol": "SHFE.rb2610"}
        )
        assert response.status_code == 503
        assert response.json() == {
            "detail": {"code": "SERVICE_NOT_READY", "service": "tq"}
        }
        assert client.get("/readyz").json()["services"]["tq"] == "failed"
        assert request_service(client, "kraken").status_code == 200


def test_runtime_network_failure_is_uniform_but_can_recover_on_next_request(
    environment,
):
    with environment() as (client, _, _, _, _, raw, _):
        wait_for_states(client, dict.fromkeys(IDENTITIES.values(), "ready"))
        raw["binance"].error = ccxt.NetworkError("private-upstream-sentinel")
        response = request_service(client, "binance")
        assert response.status_code == 503
        assert response.json() == {
            "detail": {"code": "SERVICE_NOT_READY", "service": IDENTITIES["binance"]}
        }
        assert request_service(client, "kraken").status_code == 200
        raw["binance"].error = ccxt.AuthenticationError("private-upstream-sentinel")
        response = request_service(client, "binance")
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "PROVIDER_AUTH_FAILED"
        raw["binance"].error = None
        assert request_service(client, "binance").status_code == 200


def test_closed_ccxt_identity_does_not_remain_ready_or_close_other_clients(environment):
    from src.router import trader_router

    with environment() as (client, _, _, _, _, raw, _):
        wait_for_states(client, dict.fromkeys(IDENTITIES.values(), "ready"))
        trader_router.exchange_manager.get_client("binance", "future", "live").close()
        response = request_service(client, "binance")
        assert response.status_code == 503
        assert response.json() == {
            "detail": {"code": "SERVICE_NOT_READY", "service": IDENTITIES["binance"]}
        }
        assert (
            client.get("/readyz").json()["services"][IDENTITIES["binance"]] == "failed"
        )
        assert request_service(client, "kraken").status_code == 200
        assert raw["kraken"].closed == 0
