"""共享网络沿正式部署 Shell 创建；本地、远端与 serve 均不调用真实 Podman。"""

import copy
import shutil
import subprocess

import pytest

from scripts.container_common import NAME
from src.cfb.layout import layout
from src.tools.config_loader import load_config
from Test.helpers.container_shell import ROOT
from Test.helpers.container_shell import engine as engine
from Test.helpers.container_shell import source as source
from Test.test_cfb_deployment import build

NETWORK_CREATE = ('network', 'create', '--ignore', 'trading-net')


def activate(engine, source, profile):
    selected = source
    if profile == 'local':
        engine.root.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / 'market_data.toml', engine.root / 'market_data.toml')
        selected = source / 'config.toml'
    return engine.run('activate', selected, profile=profile)


@pytest.mark.parametrize('profile', ['local', 'remote'])
@pytest.mark.parametrize('existing', [False, True])
def test_network_created_or_reused_without_reconfiguring(engine, source, profile, existing):
    network = {'name': 'trading-net', 'driver': 'bridge', 'subnet': '10.90.0.0/24',
               'dns_enabled': False, 'labels': {'owner': 'other-project'}}
    state = engine.read()
    if existing:
        state.networks['trading-net'] = copy.deepcopy(network)
    engine.write(state)

    result = activate(engine, source, profile)
    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert NETWORK_CREATE in state.events
    create = next(item for item in state.events if item[0] == 'create')
    assert '--network=trading-net' in create
    assert state.events.index(NETWORK_CREATE) < state.events.index(create)
    assert create[create.index('--publish') + 1] == '127.0.0.1:5123:5123'
    assert 'trading-net' in state.containers[NAME]['NetworkSettings']['Networks']
    if existing:
        assert state.networks['trading-net'] == network
    saved_networks = copy.deepcopy(state.networks)
    state.events.clear()
    engine.write(state)

    result = activate(engine, source, profile)
    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert state.networks == saved_networks
    assert not any(item[0] in {'create', 'start', 'stop', 'rename', 'rm'} for item in state.events)
    assert engine.run('stop').returncode == 0
    assert engine.read().networks == saved_networks


def test_network_failure_leaves_existing_instances_untouched(engine, source):
    build(engine, source)
    assert engine.run('start').returncode == 0
    state = engine.read()
    before = copy.deepcopy(state.containers)
    state.fail_network = True
    state.events.clear()
    engine.write(state)

    result = engine.run('start')

    assert result.returncode != 0
    assert 'trading-net' in result.stderr
    state = engine.read()
    assert state.containers == before
    assert not any(item[0] in {'stop', 'rename', 'create', 'rm'} or '--detach' in item for item in state.events)


@pytest.mark.parametrize('target', [NAME, 'ccxt-proxy2-cfb-sandbox', 'ccxt-proxy2-cfb-live'])
def test_missing_network_replaces_only_the_affected_instance(engine, source, target):
    build(engine, source)
    assert engine.run('start').returncode == 0
    state = engine.read()
    state.containers[target]['NetworkSettings']['Networks'] = None
    state.events.clear()
    engine.write(state)
    evidence = engine.root / 'data' / 'network-migration-evidence'
    evidence.write_bytes(b'keep-database-and-journal')

    result = engine.run('start')

    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert all('trading-net' in (item['NetworkSettings']['Networks'] or {})
               for item in state.containers.values())
    stopped = [item[-1] for item in state.events if item[0] == 'stop']
    assert stopped == [target]
    assert evidence.read_bytes() == b'keep-database-and-journal'
    assert len(state.containers) == 3


@pytest.mark.parametrize('failure', [False, True])
def test_host_serve_prepares_the_same_network(engine, source, failure):
    build(engine, source)
    uploaded = (engine.root / '.container/uploaded').read_text().splitlines()[0]
    shutil.copytree(engine.root / '.container/sources' / uploaded / 'files', engine.root, dirs_exist_ok=True)
    state = engine.read()
    state.events.clear()
    state.fail_network = failure
    engine.write(state)
    config_path = source / 'config.toml'
    modes = layout(load_config(config_path, profile='dev'))

    result = subprocess.run(
        ['sh', str(ROOT / 'scripts/container_cfb_host.sh'), str(engine.root), str(config_path),
         'dev', modes, '0'], env=engine.env, capture_output=True, text=True, timeout=20,
    )

    state = engine.read()
    assert NETWORK_CREATE in state.events
    if failure:
        assert result.returncode != 0 and 'trading-net' in result.stderr
        assert not state.containers
    else:
        assert result.returncode == 0, result.stderr
        assert set(state.containers) == {'ccxt-proxy2-cfb-sandbox', 'ccxt-proxy2-cfb-live'}
        assert all('trading-net' in item['NetworkSettings']['Networks'] for item in state.containers.values())
        starts = [item for item in state.events if '--detach' in item]
        assert all('--network=trading-net' in item for item in starts)
        assert state.events.index(NETWORK_CREATE) < state.events.index(starts[0])
