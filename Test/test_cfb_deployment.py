"""运行正式部署 Shell 验证配套执行器，不调用真实 Podman 或账户。"""

from scripts import cfb_host, serve
from scripts.container_common import DeploymentError
from src.tools.config_loader import load_config
from Test.helpers.container_engine import CFB_IMAGE, NEW_IMAGE
from Test.helpers.container_shell import ROOT, write_upload
from Test.helpers.container_shell import engine as engine
from Test.helpers.container_shell import source as source


def enable(source):
    path = source / "config.toml"
    path.write_text('SECRET = "offline-only"\n[[service_whitelist]]\nservice = "cfb"\n[cfb]\nvnc_enabled = true\n')


def build(engine, source, *, success=True):
    enable(source)
    files = {str(path.relative_to(ROOT)): path.read_bytes()
             for folder in (ROOT / 'src/cfb', ROOT / 'containers/cfb/native', ROOT / 'containers/cfb/container')
             for path in folder.rglob('*') if path.is_file() and path.suffix in {'.py', '.toml', '.c', '.h', '.reg', '.xml'}}
    for name in ('base_types', 'tools/config_types', 'tools/config_loader', 'tools/config_profiles',
                 'tools/market_data_types', 'tools/deployment_types'):
        files[f'src/{name}.py'] = (ROOT / f'src/{name}.py').read_bytes()
    write_upload(source, files)
    result = engine.run('upload-build', source)
    if success:
        assert result.returncode == 0, result.stderr
    return result


def test_build_prepares_pair_without_start_then_start_reuses_and_stop_stops_both(engine, source):
    build(engine, source)
    state = engine.read()
    assert not state.containers
    mapping = engine.root / '.container/cfb-images' / NEW_IMAGE.removeprefix('sha256:')
    assert mapping.read_text().strip() == CFB_IMAGE
    result = engine.run('start')
    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert set(state.containers) == {'ccxt-proxy2', 'ccxt-proxy2-cfb'}
    cfb = state.containers['ccxt-proxy2-cfb']
    assert cfb['Image'] == CFB_IMAGE and cfb['State']['Running']
    event = next(item for item in state.events if '--detach' in item)
    assert '127.0.0.1:45174:45174' in event and '127.0.0.1:45175:45175' in event
    assert not any('45173' in value for value in event)
    assert f'{engine.root / "data/cfb"}:/data:rw' in event
    state.events.clear()
    engine.write(state)
    assert engine.run('start').returncode == 0
    assert not any('--detach' in item or item[0] in {'stop', 'rename'} for item in engine.read().events)
    assert engine.run('stop').returncode == 0
    assert all(not item['State']['Running'] for item in engine.read().containers.values())


def test_cfb_failure_leaves_main_available(engine, source):
    build(engine, source)
    state = engine.read()
    state.fail_image = CFB_IMAGE
    engine.write(state)
    result = engine.run('start')
    assert result.returncode == 0, result.stderr
    assert 'CFB' in result.stderr
    assert engine.read().containers['ccxt-proxy2']['State']['Running']
    assert 'ccxt-proxy2-cfb' not in engine.read().containers


def test_cfb_build_failure_does_not_publish_main(engine, source):
    state = engine.read()
    state.fail_cfb_build = True
    engine.write(state)
    result = build(engine, source, success=False)
    assert result.returncode != 0 and 'CFB' in result.stderr
    assert not (engine.root / '.container/prepared').exists()
    assert not engine.read().containers


def test_failed_replacement_restores_old_cfb_and_preserves_journal(engine, source):
    build(engine, source)
    assert engine.run('start').returncode == 0
    journal = engine.root / 'data/cfb/state/operations.sqlite3'
    journal.parent.mkdir()
    journal.write_bytes(b'preserve-unknown-submission')
    state = engine.read()
    prior = 'sha256:' + '9' * 64
    state.images[prior] = state.image(prior)
    state.images[prior]['Labels']['io.ccxt-proxy2.kind'] = 'cfb-runtime'
    state.containers['ccxt-proxy2-cfb']['Image'] = prior
    state.fail_image = CFB_IMAGE
    engine.write(state)
    result = engine.run('start')
    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert state.containers['ccxt-proxy2-cfb']['Image'] == prior
    assert state.containers['ccxt-proxy2-cfb']['State']['Running']
    assert 'ccxt-proxy2-cfb-replacing' not in state.containers
    assert journal.read_bytes() == b'preserve-unknown-submission'


def test_serve_failure_is_isolated_and_disabled_does_not_use_podman(tmp_path, monkeypatch):
    config = tmp_path / 'config.toml'
    config.write_text('SECRET="offline"\n')
    calls = []
    monkeypatch.setattr(cfb_host, 'require_runtime', lambda: calls.append('podman'))
    cfb_host.ensure_cfb(config, load_config(config), 'dev')
    assert calls == []
    monkeypatch.setattr(serve.os, 'execvp', lambda *args: calls.append('uvicorn'))
    monkeypatch.setattr(cfb_host, 'ensure_cfb', lambda *args: (_ for _ in ()).throw(DeploymentError('offline failure')))
    assert serve.main(['--config', str(config)]) == 0
    assert calls == ['uvicorn']


def test_host_prepare_uses_shared_script_and_stop_generation(tmp_path, monkeypatch):
    enable(tmp_path)
    path = tmp_path / 'config.toml'
    monkeypatch.setattr(cfb_host, 'ROOT', tmp_path)
    monkeypatch.setattr(cfb_host, 'require_runtime', lambda: None)
    calls = []
    monkeypatch.setattr(cfb_host, 'command', lambda args, **kwargs: calls.append(args))
    cfb_host.ensure_cfb(path, load_config(path), 'dev')
    assert calls[0][0] == 'sh' and str(calls[0][1]).endswith('container_cfb_host.sh')
    assert calls[0][-3] == 'dev' and calls[0][-1] == '0'
