"""白名单必填布尔值与两处内联数组；SDK 和持久化身份保持原样。"""

import copy
import tomllib
from pathlib import Path
from typing import Any

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.tools.config_loader import load_config
from src.tools.config_profiles import apply_profile
from src.tools.config_types import AppConfig
from src.tools.service_runtime import ServiceRuntime

BASE: dict[str, Any] = {
    'SECRET': 'offline-only',
    'binance': {'test': {'api_key': 'offline', 'secret': 'offline'},
                'live': {'api_key': 'offline', 'secret': 'offline'}},
    'cfb': {'accounts': {'live': {'broker_id': '6020', 'site': '一套'}}},
}


@pytest.mark.parametrize('service', ['ccxt', 'cfb', 'ctp'])
@pytest.mark.parametrize('value', [None, 0, 1, '', 'true', 'false', 'live', 'sandbox'])
def test_environment_must_be_a_boolean_before_provider_validation(service, value):
    item = {'service': service, 'is_live': value}
    if service == 'ccxt':
        item.update(exchange='binance', market='future')
    with pytest.raises(ValidationError) as error:
        AppConfig.model_validate(BASE | {'service_whitelist': [item]})
    assert any(e['type'] == 'bool_type' and e['loc'][-1] == 'is_live' for e in error.value.errors())


@pytest.mark.parametrize('item, field, error_type', [
    ({'service': 'cfb'}, 'is_live', 'missing'),
    ({'service': 'cfb', 'mode': 'live'}, 'mode', 'extra_forbidden'),
    ({'service': 'cfb', 'is_live': True, 'mode': 'live'}, 'mode', 'extra_forbidden'),
    ({'service': 'cfb', 'trading_env': 'live'}, 'trading_env', 'extra_forbidden'),
    ({'service': 'tq', 'is_live': True}, 'is_live', 'extra_forbidden'),
])
def test_missing_or_retired_fields_are_rejected(item, field, error_type):
    with pytest.raises(ValidationError) as error:
        AppConfig.model_validate(BASE | {'service_whitelist': [item]})
    assert any(e['type'] == error_type and e['loc'][-1] == field for e in error.value.errors())


def test_false_is_explicit_sandbox_and_both_identities_remain_distinct():
    config = AppConfig.model_validate(BASE | {'service_whitelist': [
        {'service': 'cfb', 'is_live': False}, {'service': 'cfb', 'is_live': True}]})
    assert [item.identity for item in config.service_whitelist] == ['cfb/sandbox', 'cfb/live']
    for item, mode in zip(config.service_whitelist, ('sandbox', 'live'), strict=True):
        assert item.service == 'cfb'
        assert item.mode == mode
    assert config.model_dump()['service_whitelist'] == [
        {'service': 'cfb', 'is_live': False}, {'service': 'cfb', 'is_live': True}]
    with pytest.raises(ValidationError, match='duplicate service_whitelist'):
        AppConfig.model_validate(BASE | {'service_whitelist': [{'service': 'cfb', 'is_live': False}] * 2})


def test_remote_replaces_complete_whitelist_and_disabled_environment_is_rejected(tmp_path):
    payload = copy.deepcopy(BASE)
    payload['ohlcv_cache'] = {'database_path': str(tmp_path / 'cache.duckdb')}
    payload['service_whitelist'] = [
        {'service': 'ccxt', 'exchange': 'binance', 'market': 'future', 'is_live': flag}
        for flag in (False, True)] + [{'service': 'cfb', 'is_live': flag} for flag in (False, True)]
    payload['overrides'] = {'remote': {'service_whitelist': [
        item for item in payload['service_whitelist'] if item['is_live']]}}
    original = copy.deepcopy(payload)
    local = AppConfig.model_validate(apply_profile(payload, 'local'))
    remote = AppConfig.model_validate(apply_profile(payload, 'remote'))
    assert len(local.service_whitelist) == 4
    assert [item.identity for item in remote.service_whitelist] == ['ccxt/binance/future/live', 'cfb/live']
    assert payload == original
    runtime = ServiceRuntime(remote)
    for identity in ('ccxt/binance/future/sandbox', 'cfb/sandbox'):
        with pytest.raises(HTTPException) as caught:
            runtime.require(identity)
        assert caught.value.status_code == 503
        assert caught.value.detail == {'code': 'SERVICE_NOT_ENABLED', 'service': identity}
    runtime.close()


def test_example_keeps_both_inline_arrays_at_correct_toml_scope():
    path = Path('config.example.toml')
    text = path.read_text()
    raw = tomllib.loads(text)
    assert '[[service_whitelist]]' not in text
    assert text.count('service_whitelist = [') == 2
    assert raw['service_whitelist'] == [{'service': 'cfb', 'is_live': False}, {'service': 'cfb', 'is_live': True}]
    assert raw['overrides']['remote']['service_whitelist'] == [{'service': 'cfb', 'is_live': True}]
    assert all(type(item['is_live']) is bool for item in raw['service_whitelist'])
    for profile in ('dev', 'local'):
        assert len(load_config(path, profile=profile).service_whitelist) == 2
