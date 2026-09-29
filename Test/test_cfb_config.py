"""合并配置、场景差异和 TCP 出口严格分支，不读取私有文件。"""

import os
from pathlib import Path

import pytest

from src.cfb.config import Settings
from src.cfb.layout import layout
from src.cfb.network import parse_proxy, prepare_proxy, process_environment
from src.tools.config_loader import ConfigError, load_config


def test_config_keeps_accounts_and_applies_remote_proxy(tmp_path):
    path = tmp_path / 'config.toml'
    path.write_text('''SECRET="offline"\n[[service_whitelist]]\nservice="cfb"
[proxy]
http="http://127.0.0.1:9998"
[cfb]
enable_proxy=false
[cfb.accounts.sandbox]
username="offline-sample"
password="offline-only"
[overrides.remote.cfb]
enable_proxy=true
''')
    for profile in ('dev', 'local', 'remote'):
        config = load_config(path, profile=profile)
        assert config.cfb is not None
        assert config.cfb.enable_proxy == (profile == 'remote')
        assert config.cfb.account.username.get_secret_value() == 'offline-sample'
        assert config.cfb.bridge.mode == 'sandbox'
        output = layout(config)
        assert 'offline' not in output and len(output.split()) == 5


@pytest.mark.parametrize('field', ['base_url="http://127.0.0.1:45173"', 'api={port=45173}',
                                  'bridge={data_dir="/outside"}', 'enable_proxy=true'])
def test_retired_fields_and_invalid_runtime_are_rejected(tmp_path, field):
    path = tmp_path / 'config.toml'
    path.write_text('SECRET="offline"\n[[service_whitelist]]\nservice="cfb"\n[cfb]\n' + field)
    with pytest.raises(ConfigError):
        load_config(path, profile='dev')


def test_layout_ignores_other_sdk_and_backup_accounts(tmp_path):
    path = tmp_path / 'config.toml'
    base = 'SECRET="offline"\n[[service_whitelist]]\nservice="cfb"\n[cfb]\n'
    path.write_text(base)
    before = layout(load_config(path, profile='dev'))
    path.write_text(base + '[cfb.accounts.live]\nusername="backup"\npassword="ignored"\n[binance]\nenable_proxy=false\n')
    assert layout(load_config(path, profile='dev')) == before
    path.write_text(base + '[cfb.accounts.sandbox]\nusername="selected"\npassword="changed"\n')
    assert layout(load_config(path, profile='dev')) != before


@pytest.mark.parametrize('value', [None, '', 'https://proxy:8', 'socks5://proxy:9', 'http://proxy/path',
                                  'http://user@proxy', 'http://proxy:99999', 'http://u:p%20x@proxy'])
def test_unrepresentable_proxy_does_not_fall_back(value):
    with pytest.raises(ValueError, match='CONNECT'):
        parse_proxy(value)


def test_proxy_file_private_and_disabled_environment_is_clean(tmp_path, monkeypatch):
    libraries = []
    for index in (1, 2):
        library = tmp_path / f'lib{index}'
        library.write_bytes(b'\x7fELF' + bytes([index]))
        libraries.append(library)
    monkeypatch.setattr('src.cfb.network.PROXY_LIBRARIES', libraries)
    settings = Settings.model_validate({'bridge': {'data_dir': str(tmp_path)}})
    inherited = dict(os.environ, HTTP_PROXY='must-not-inherit', LD_PRELOAD='unrelated.so')
    assert 'HTTP_PROXY' not in process_environment(settings, inherited)
    assert 'LD_PRELOAD' not in process_environment(settings, inherited)
    prepare_proxy(settings, 'http://u:p@127.0.0.1:8888')
    env = process_environment(settings, inherited)
    assert env['LD_PRELOAD'] == 'libproxychains.so.4'
    config = Path(env['PROXYCHAINS_CONF_FILE'])
    assert config.stat().st_mode & 0o777 == 0o600
    assert 'strict_chain' in config.read_text() and 'http 127.0.0.1 8888 u p' in config.read_text()
    libraries[0].write_bytes(b'\x7fELF\x02')
    with pytest.raises(ValueError, match='architecture'):
        prepare_proxy(settings, 'http://127.0.0.1:8888')
