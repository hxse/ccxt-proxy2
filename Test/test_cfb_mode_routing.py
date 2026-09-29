"""八条业务路径和诊断统一按已校验 mode 分发，不回退另一账户。"""

import asyncio

import pytest
from fastapi import FastAPI, HTTPException

from src.cfb.errors import BridgeError, ServiceStatus
from src.cfb.ipc import Response
from src.cfb.manager import Manager
from src.router.auth_handler import manager as authentication
from src.router.cfb_router import cfb_router
from src.tools.config_types import CfbConfig
from Test.test_ctp_http import LocalClient

ORDER = {'exchange_id': 'DCE', 'instrument_id': 'm2701', 'side': 'buy', 'offset': 'open', 'volume': 1}
OPERATIONS = [
    ('post', 'create_market_order', ORDER),
    ('post', 'create_limit_order', ORDER | {'price': 3000}),
    ('post', 'cancel_order', {'by': 'exchange_order', 'exchange_id': 'DCE', 'instrument_id': 'm2701', 'order_sys_id': '123'}),
    ('get', 'fetch_balance', {}), ('get', 'fetch_orders', {}),
    ('get', 'fetch_trades', {}), ('get', 'fetch_positions', {}),
    ('get', 'fetch_trading_status', {'exchange_id': 'DCE', 'product_id': 'm'}),
]


def setup(monkeypatch, disabled=None, failed=None):
    registry = Manager()
    config = CfbConfig.model_validate({'accounts': {'live': {'broker_id': '6020', 'site': '一套'}}})
    calls = []
    for mode in ('sandbox', 'live'):
        if mode == disabled:
            continue
        registry.initialize(config, mode)

        async def call(kind, *, _mode=mode, **kwargs):
            calls.append((_mode, kind, kwargs))
            if _mode == failed:
                raise BridgeError('SERVICE_NOT_READY', 'offline failure')
            if kind == 'status':
                return Response(status=200, body=ServiceStatus(request_mode=_mode).model_dump(mode='json'))
            return Response(status=200, body={'mode': _mode})

        monkeypatch.setattr(registry.get(mode), 'call', call)

    class Runtime:
        def require(self, identity):
            if identity == f'cfb/{disabled}':
                raise HTTPException(503, {'code': 'SERVICE_NOT_ENABLED', 'service': identity})

    app = FastAPI()
    app.state.service_runtime = Runtime()
    app.dependency_overrides[authentication] = lambda: {'sub': 'offline'}
    monkeypatch.setattr('src.router.cfb_router.cfb_manager', registry)
    app.include_router(cfb_router)
    return LocalClient(app), registry, calls


@pytest.mark.parametrize('mode', ['sandbox', 'live'])
@pytest.mark.parametrize('method,path,parameters', OPERATIONS)
def test_all_business_routes_select_requested_mode(monkeypatch, mode, method, path, parameters):
    http, registry, calls = setup(monkeypatch)
    result = (http.get('/cfb/' + path, params=parameters | {'is_live': mode == 'live'}) if method == 'get'
              else http.post('/cfb/' + path, json=parameters | {'is_live': mode == 'live'}))
    assert result.status_code == 200, result.text
    assert result.json() == {'mode': mode}
    assert len(calls) == 1 and calls[0][0] == mode
    assert calls[0][2]['operation'].parameters['is_live'] is (mode == 'live')
    asyncio.run(registry.close())


@pytest.mark.parametrize('disabled,failed', [('live', None), (None, 'live'), ('sandbox', None), (None, 'sandbox')])
def test_unavailable_target_never_falls_back(monkeypatch, disabled, failed):
    http, registry, calls = setup(monkeypatch, disabled, failed)
    target = disabled or failed
    result = http.get('/cfb/fetch_balance', params={'is_live': target == 'live'})
    assert result.status_code == 503
    if disabled:
        assert result.json()['detail'] == {'code': 'SERVICE_NOT_ENABLED', 'service': f'cfb/{target}'}
        assert not calls
    else:
        assert result.json()['error']['code'] == 'SERVICE_NOT_READY'
        assert [item[0] for item in calls] == [target]
    other = 'sandbox' if target == 'live' else 'live'
    assert http.get('/cfb/fetch_balance', params={'is_live': other == 'live'}).json() == {'mode': other}
    asyncio.run(registry.close())


def test_required_environment_diagnostics_and_invalid_modes(monkeypatch):
    http, registry, calls = setup(monkeypatch)
    assert http.get('/cfb/fetch_balance').status_code == 422
    assert http.post('/cfb/create_limit_order', json=ORDER | {'price': 3000}).status_code == 422
    assert http.get('/cfb/status?is_live=true').json()['request_mode'] == 'live'
    assert http.get('/cfb/readyz?is_live=false').status_code == 503
    assert http.get('/cfb/healthz?is_live=true').json() == {'mode': 'live'}
    calls.clear()
    assert http.get('/cfb/fetch_balance?mode=invalid').status_code == 422
    assert http.post('/cfb/create_limit_order?is_live=true', json=ORDER | {'price': 3000, 'mode': 'sandbox'}).status_code == 422
    assert not calls
    asyncio.run(registry.close())
