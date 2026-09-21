import asyncio

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.tools.cfb_proxy import CfbProxy
from src.tools.config_types import AppConfig, CfbConfig
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan
from Test.test_service_lifecycle import fake_managers


@pytest.mark.parametrize(
    "payload",
    [
        {"service_whitelist": [{"service": "cfb"}]},
        {"cfb": {}, "service_whitelist": [{"service": "cfb", "mode": "live"}]},
        {"cfb": {}, "service_whitelist": [{"service": "cfb"}, {"service": "cfb"}]},
        {"cfb": {"base_url": "file:///tmp/cfb"}},
        {"cfb": {"base_url": "http://user:password@localhost:45173"}},
        {"cfb": {"base_url": "http://localhost:45173/?mode=live"}},
        {"cfb": {"request_timeout_seconds": 0}},
        {"cfb": {"request_timeout_seconds": float("inf")}},
    ],
)
def test_cfb_configuration_rejects_invalid_values(payload):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"SECRET": "offline"} | payload)


@pytest.mark.parametrize("enabled", [False, True])
def test_lifespan_initializes_only_enabled_cfb_from_startup_snapshot_and_closes_it(
    monkeypatch, enabled
):
    config = AppConfig.model_validate(
        {
            "SECRET": "offline",
            "cfb": {
                "base_url": "http://original.invalid",
                "request_timeout_seconds": 27,
            },
            "service_whitelist": [{"service": "cfb"}] if enabled else [],
        }
    )
    runtime = ServiceRuntime(config)
    config.cfb = CfbConfig(base_url="http://edited.invalid", request_timeout_seconds=1)
    config.service_whitelist.clear()
    proxy = CfbProxy()
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.cfb_proxy.cfb_proxy", proxy)
    monkeypatch.setattr(
        "src.tools.telegram_manager.telegram_manager.close", lambda: None
    )
    created = []

    def create_client(**kwargs):
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: pytest.fail("startup must not request CFB or login")
            ),
            **kwargs,
        )
        created.append(client)
        return client

    monkeypatch.setattr("src.tools.cfb_proxy.AsyncClient", create_client)

    async def run():
        async with lifespan(FastAPI()):
            assert runtime.ready and runtime.initialized == (["cfb"] if enabled else [])
            if enabled:
                assert len(created) == 1 and not created[0].is_closed
                assert str(created[0].base_url) == "http://original.invalid/"
                assert created[0].timeout.read == 27
            else:
                assert created == [] and proxy._client is None
        assert not runtime.ready and proxy._client is None
        assert all(client.is_closed for client in created)

    asyncio.run(run())


def test_later_sdk_startup_failure_also_closes_cfb_client(monkeypatch):
    config = AppConfig.model_validate(
        {
            "SECRET": "offline",
            "cfb": {},
            "binance": {"test": {"api_key": "offline", "secret": "offline"}},
            "service_whitelist": [
                {"service": "cfb"},
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    "mode": "sandbox",
                },
            ],
        }
    )
    runtime = ServiceRuntime(config)
    proxy = CfbProxy()
    ccxt, _, _ = fake_managers([], fail="ccxt")
    created = []

    def create_client(**kwargs):
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(200)), **kwargs
        )
        created.append(client)
        return client

    monkeypatch.setattr("src.tools.cfb_proxy.AsyncClient", create_client)
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", ccxt)
    monkeypatch.setattr("src.tools.cfb_proxy.cfb_proxy", proxy)
    monkeypatch.setattr(
        "src.tools.telegram_manager.telegram_manager.close", lambda: None
    )

    async def run():
        with pytest.raises(RuntimeError, match="offline initialization failed"):
            async with lifespan(FastAPI()):
                pytest.fail("failed startup accepted requests")

    asyncio.run(run())
    assert len(created) == 1 and created[0].is_closed
    assert proxy._client is None and not runtime.ready
