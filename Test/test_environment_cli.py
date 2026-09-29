"""命名布尔参数及 Just 透传；CLI 测试不访问 Podman、账户或 GUI。"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from debug import trade_action
from script import ctp_assessment
from src.cfb import daemon


@pytest.mark.parametrize("module,extra", [(ctp_assessment, []), (daemon, [])])
@pytest.mark.parametrize("args", [[], ["--mode=live"], ["--is-live=1"], ["--is-live=False"], ["--is-live"]])
def test_invalid_environment_precedes_configuration(monkeypatch, module, extra, args):
    def forbidden(*args, **kwargs):
        pytest.fail("参数非法时不能加载私有配置")

    monkeypatch.setattr(module, "load_config", forbidden)
    monkeypatch.setattr(sys, "argv", ["test-cli", *extra, *args])
    with pytest.raises(SystemExit) as caught:
        module.main()
    assert caught.value.code == 2


@pytest.mark.parametrize("flag,expected", [("true", "live"), ("false", "sandbox")])
def test_daemon_cli_normalizes_environment_once(monkeypatch, flag, expected):
    calls = []

    async def run(path, mode):
        calls.append((path, mode))

    monkeypatch.setattr(daemon, "run", run)
    monkeypatch.setattr(sys, "argv", ["daemon", "--is-live=" + flag])
    assert daemon.main() == 0
    assert calls == [(Path("/app/config.toml"), expected)]
    monkeypatch.setattr(sys, "argv", ["trade", "balance", "--is-live=" + flag])
    args = trade_action.parse_args()
    assert trade_action.base_payload(args)["is_live"] is (flag == "true")
    assert "mode" not in trade_action.base_payload(args)


@pytest.mark.parametrize("args", [[], ["--mode=live"], ["--is-live=0"], ["--is-live"]])
def test_trade_debug_requires_explicit_boolean(monkeypatch, args):
    monkeypatch.setattr(sys, "argv", ["trade", "balance", *args])
    with pytest.raises(SystemExit) as caught:
        trade_action.parse_args()
    assert caught.value.code == 2


@pytest.mark.parametrize("command,action,arguments", [
    (["debug-balance"], "balance", []),
    (["debug-open-long", "0.005"], "open-long", ["--amount", "0.005"]),
    (["debug-stop-loss-long", "40000", "0.01"], "stop-loss-long", ["--amount", "0.01", "--trigger-price", "40000"]),
    (["debug-set-margin-mode", "isolated"], "set-margin-mode", ["--margin-mode", "isolated"]),
])
def test_shortcuts_forward_environment_and_business_arguments(tmp_path, command, action, arguments):
    tools = tmp_path / "bin"
    tools.mkdir()
    capture = tmp_path / "argv"
    uv = tools / "uv"
    uv.write_text('#!/bin/sh\nprintf \'%s\\n\' "$@" > "$CAPTURE"\n')
    uv.chmod(0o700)
    result = subprocess.run(["just", *command, "--is-live=false"],
                            env={**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"], "CAPTURE": str(capture)},
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert capture.read_text().splitlines() == ["run", "--no-sync", "python", "debug/trade_action.py",
                                              action, *arguments, "--is-live=false"]
