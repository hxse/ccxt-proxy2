"""CFB 两种模式独立初始化与关闭；业务不触发终端初始化。"""

import asyncio

import pytest
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from src.base_types import ModeType
from src.cfb.manager import Manager
from src.tools.config_types import AppConfig, CfbConfig
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan

MODES: list[ModeType] = ['sandbox', 'live']
CFB = {'accounts': {'live': {'broker_id': '6020', 'site': '一套'}}}


@pytest.mark.parametrize('items', [
    [{'service': 'cfb'}],
    [{'service': 'cfb', 'is_live': 'third'}],
    [{'service': 'cfb', 'is_live': False}] * 2,
    [{'service': 'cfb', 'is_live': (x == "live")} for x in ('sandbox', 'live', 'live')],
])
def test_whitelist_requires_unique_explicit_mode(items):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({'SECRET': 'offline', 'cfb': CFB, 'service_whitelist': items})


@pytest.mark.parametrize('field', [{'base_url': 'http://old.invalid'}, {'api': {'port': 45173}},
                                 {'bridge': {'mode': 'sandbox'}}, {'vnc': {'port': 45174}},
                                 {'request_timeout_seconds': 0}, {'request_timeout_seconds': float('inf')}])
def test_retired_or_invalid_public_config_is_rejected(field):
    with pytest.raises(ValidationError):
        AppConfig.model_validate({'SECRET': 'offline', 'cfb': field})


@pytest.mark.parametrize('enabled', [[], ['sandbox'], ['live'], MODES])
def test_lifespan_keeps_mode_snapshot_and_closes_each_client(monkeypatch, tmp_path, enabled):
    config = AppConfig.model_validate({'SECRET': 'offline', 'cfb': CFB | {'request_timeout_seconds': 27.0},
        'ohlcv_cache': {'database_path': str(tmp_path / 'cache.duckdb')},
        'service_whitelist': [{'service': 'cfb', 'is_live': (mode == "live")} for mode in enabled]})
    runtime, registry = ServiceRuntime(config), Manager(tmp_path / 'cfb')
    config.cfb = CfbConfig(request_timeout_seconds=1.0)
    config.service_whitelist.clear()
    monkeypatch.setattr('src.tools.shared.service_runtime', runtime)
    monkeypatch.setattr('src.cfb.manager.cfb_manager', registry)
    monkeypatch.setattr('src.tools.telegram_manager.telegram_manager.close', lambda: None)
    monkeypatch.setattr(asyncio, 'open_unix_connection', lambda *a, **kw: pytest.fail('初始化不应连接或登录'))

    async def run():
        clients = []
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            assert set(runtime.initialized) == {f'cfb/{mode}' for mode in enabled}
            for mode in MODES:
                assert registry.is_ready(mode) == (mode in enabled)
                if mode in enabled:
                    client = registry.get(mode)
                    clients.append(client)
                    assert client.timeout == 27 and client.settings.request_mode == mode
                    assert client.path == tmp_path / 'cfb' / mode / 'run/bridge.sock'
        assert not runtime.ready
        assert all(not client.is_ready() and not client.writers for client in clients)
        assert all(not registry.is_ready(mode) for mode in MODES)
    asyncio.run(run())


@pytest.mark.parametrize('failed', MODES)
def test_initialization_failure_is_isolated_between_modes(monkeypatch, tmp_path, failed):
    config = AppConfig.model_validate({'SECRET': 'offline', 'cfb': CFB,
        'ohlcv_cache': {'database_path': str(tmp_path / 'cache.duckdb')},
        'service_whitelist': [{'service': 'cfb', 'is_live': (mode == "live")} for mode in MODES]})
    runtime, registry = ServiceRuntime(config), Manager()
    initialize = registry.initialize

    def start(settings, mode):
        if mode == failed:
            raise OSError('offline failure')
        initialize(settings, mode)

    monkeypatch.setattr(registry, 'initialize', start)
    monkeypatch.setattr('src.tools.shared.service_runtime', runtime)
    monkeypatch.setattr('src.cfb.manager.cfb_manager', registry)
    monkeypatch.setattr('src.tools.telegram_manager.telegram_manager.close', lambda: None)

    async def run():
        async with lifespan(FastAPI()):
            await asyncio.to_thread(runtime.wait_for_startup)
            for mode in MODES:
                if mode == failed:
                    with pytest.raises(HTTPException) as caught:
                        runtime.require(f'cfb/{mode}')
                    assert caught.value.status_code == 503
                    assert caught.value.detail == {'code': 'SERVICE_NOT_READY', 'service': f'cfb/{mode}'}
                else:
                    runtime.require(f'cfb/{mode}')
    asyncio.run(run())
