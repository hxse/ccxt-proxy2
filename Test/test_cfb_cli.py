"""用户运维入口必须明确模式，且只允许操作匹配的本项目实例。"""

import json

import pytest

from scripts import cfb_cli


@pytest.mark.parametrize('args', [['--status'], ['--is-live=third', '--status'], ['--mode=live', '--status'], ['--is-live=1', '--status'], ['--is-live', '--status']])
def test_mode_required_before_accessing_runtime(monkeypatch, args):
    monkeypatch.setattr(cfb_cli, 'command', lambda *a, **kw: pytest.fail('无效模式不能访问 Podman'))
    with pytest.raises(SystemExit) as error:
        cfb_cli.main(args)
    assert error.value.code == 2


@pytest.mark.parametrize('mode', ['sandbox', 'live'])
def test_explicit_mode_selects_only_matching_container(monkeypatch, tmp_path, mode, capsys):
    calls = []
    monkeypatch.setattr(cfb_cli, 'ROOT', tmp_path)

    def command(args, **kwargs):
        calls.append(args)
        if 'inspect' in args:
            return f'cfb|{tmp_path}|{mode}\n'
        return json.dumps({'request_mode': mode})

    monkeypatch.setattr(cfb_cli, 'command', command)
    assert cfb_cli.main([f'--is-live={str(mode == "live").lower()}', '--status']) == 0
    assert calls[0][-1] == f'ccxt-proxy2-cfb-{mode}'
    assert calls[1] == ['podman', 'exec', f'ccxt-proxy2-cfb-{mode}',
                        '/app/.venv/bin/python', '-m', 'src.cfb.ctl', 'status']
    assert json.loads(capsys.readouterr().out)['request_mode'] == mode


def test_mismatched_instance_label_blocks_operation(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(cfb_cli, 'ROOT', tmp_path)

    def command(args, **kwargs):
        calls.append(args)
        return f'cfb|{tmp_path}|sandbox'

    monkeypatch.setattr(cfb_cli, 'command', command)
    assert cfb_cli.main(['--is-live=true', '--pause']) == 1
    assert len(calls) == 1 and 'inspect' in calls[0]


@pytest.mark.parametrize('mode', ['sandbox', 'live'])
@pytest.mark.parametrize('target', ['local', 'remote'])
def test_logs_follow_managed_files_in_selected_container(monkeypatch, tmp_path, mode, target):
    import shlex
    from types import SimpleNamespace

    calls = []
    directory = str(tmp_path) if target == 'local' else '/remote/project'
    monkeypatch.setattr(cfb_cli, 'ROOT', tmp_path)
    monkeypatch.setattr(cfb_cli, 'load_deployment_config',
                        lambda **kw: SimpleNamespace(ssh_host='rn', remote_dir='dev/ccxt-proxy2'))

    def command(args, **kwargs):
        values = shlex.split(args[-1]) if args[0] == 'ssh' else args
        calls.append((values, kwargs))
        if values[0] == 'sh':
            return directory
        if 'inspect' in values:
            return f'cfb|{directory}|{mode}'
        return ''

    monkeypatch.setattr(cfb_cli, 'command', command)
    assert cfb_cli.main([f'--target={target}', f'--is-live={str(mode == "live").lower()}', '--logs']) == 0
    assert calls[-1][0] == ['podman', 'exec', f'ccxt-proxy2-cfb-{mode}',
                            '/app/.venv/bin/python', '-m', 'src.cfb.log_reader']
    assert calls[-1][1] == {'capture': False, 'timeout': None}
