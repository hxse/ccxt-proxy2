"""CFB 客户端和服务白名单的生命周期；容器管理只属于正式启动入口。"""

import asyncio

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.cfb.client import Client
from src.tools.config_types import AppConfig, CfbConfig
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan
from Test.test_service_lifecycle import fake_managers


@pytest.mark.parametrize("payload", [
    {"service_whitelist": [{"service": "cfb"}]},
    {"cfb": {}, "service_whitelist": [{"service": "cfb", "mode": "live"}]},
    {"cfb": {}, "service_whitelist": [{"service": "cfb"}, {"service": "cfb"}]},
    {"cfb": {"base_url": "http://old.invalid"}},
    {"cfb": {"api": {"port": 45173}}},
    {"cfb": {"request_timeout_seconds": 0}},
    {"cfb": {"request_timeout_seconds": float("inf")}},
    {"cfb": {"enable_proxy": True}, "service_whitelist": [{"service": "cfb"}]},
])
def test_cfb_configuration_rejects_invalid_values(payload):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({"SECRET": "offline"} | payload)


@pytest.mark.parametrize("enabled", [False, True])
def test_lifespan_uses_startup_snapshot_without_starting_terminal(monkeypatch, enabled):
    config = AppConfig.model_validate({"SECRET": "offline", "cfb": {"request_timeout_seconds": 27.0},
                                      "service_whitelist": [{"service": "cfb"}] if enabled else []})
    runtime = ServiceRuntime(config)
    config.cfb = CfbConfig(request_timeout_seconds=1.0)
    config.service_whitelist.clear()
    client = Client()
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.cfb.client.cfb_client", client)
    monkeypatch.setattr("src.tools.telegram_manager.telegram_manager.close", lambda: None)
    monkeypatch.setattr(asyncio, "open_unix_connection", lambda *a, **kw: pytest.fail("初始化不应访问终端或登录"))

    async def run():
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            assert runtime.initialized == (["cfb"] if enabled else [])
            assert client.is_ready() is enabled
            if enabled:
                assert client.timeout == 27
        assert not runtime.ready and not client.is_ready() and not client.writers
    asyncio.run(run())


def test_other_sdk_failure_does_not_close_cfb(monkeypatch, tmp_path):
    config = AppConfig.model_validate({"SECRET": "offline", "cfb": {},
        "ohlcv_cache": {"database_path": str(tmp_path / "cache.duckdb")},
        "binance": {"test": {"api_key": "offline", "secret": "offline"}},
        "service_whitelist": [{"service": "cfb"}, {"service": "ccxt", "exchange": "binance", "market": "future", "mode": "sandbox"}]})
    runtime, client = ServiceRuntime(config), Client()
    ccxt, _, _ = fake_managers([], fail="ccxt")
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", ccxt)
    monkeypatch.setattr("src.cfb.client.cfb_client", client)
    monkeypatch.setattr("src.tools.telegram_manager.telegram_manager.close", lambda: None)

    async def run():
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            runtime.require("cfb")
            assert runtime.snapshot()["services"]["ccxt/binance/future/sandbox"] == "failed"
            assert client.is_ready()
        assert not client.is_ready()
    asyncio.run(run())
