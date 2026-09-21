"""完整 VeighNa 联调入口的离线验证；GUI/网关替身不会连接账户。"""

import argparse
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from script import ctp_assessment as assessment
from src.tools.config_loader import ConfigError
from src.tools.config_types import CtpAccountConfig

PASSWORD = "offline-password"
AUTH_CODE = "offline-authcode"
ACCOUNT = "123456789"


def account(production_mode=False, **fields):
    return CtpAccountConfig(
        trader_front="tcp://assessment.invalid:12345",
        broker_id="9999",
        investor_id=ACCOUNT,
        password=PASSWORD,
        app_id="test-client",
        auth_code=AUTH_CODE,
        production_mode=production_mode,
        **fields,
    )


@pytest.mark.parametrize("production_mode", [False, True])
def test_maps_toml_to_official_gateway_fields(production_mode):
    assert assessment.gateway_setting(
        account(production_mode), "tcp://md.invalid:12346"
    ) == {
        "用户名": ACCOUNT,
        "密码": PASSWORD,
        "经纪商代码": "9999",
        "交易服务器": "tcp://assessment.invalid:12345",
        "行情服务器": "tcp://md.invalid:12346",
        "产品名称": "test-client",
        "授权编码": AUTH_CODE,
        "柜台环境": "实盘" if production_mode else "测试",
    }


def test_distinct_login_and_investor_ids_are_not_silently_reinterpreted():
    with pytest.raises(ConfigError, match="user_id 与 investor_id 一致"):
        assessment.gateway_setting(account(user_id="different-user"))
    assert assessment.gateway_setting(account(user_id=ACCOUNT))["用户名"] == ACCOUNT


@pytest.mark.parametrize(
    "value",
    ["host:123", "tcp://host:0", "tcp://host:65536", "tcp://user:password@host:1"],
)
def test_rejects_invalid_optional_market_front(value):
    with pytest.raises(argparse.ArgumentTypeError):
        assessment.market_front(value)


def write_config(tmp_path):
    path = tmp_path / "private.toml"
    path.write_text(
        f'''SECRET = "offline-assessment-secret"
[ctp.test]
trader_front = "tcp://assessment.invalid:12345"
broker_id = "9999"
investor_id = "{ACCOUNT}"
password = "{PASSWORD}"
app_id = "test-client"
auth_code = "{AUTH_CODE}"
production_mode = false
[ctp.live]
trader_front = "tcp://live.invalid:12345"
broker_id = "8888"
investor_id = "{ACCOUNT}"
password = "{PASSWORD}"
app_id = "test-client"
auth_code = "{AUTH_CODE}"
production_mode = true
''',
        encoding="utf-8",
    )
    return path


@pytest.mark.parametrize("mode", ["sandbox", "live"])
def test_default_config_is_loaded_once_and_runtime_is_private(
    tmp_path, monkeypatch, capsys, mode
):
    config_path = write_config(tmp_path)
    monkeypatch.setenv("CCXT_PROXY_CONFIG_PATH", str(config_path))
    output = tmp_path / "gui-output"
    previous_directory = Path.cwd()
    calls = []

    def gui(setting, selected_mode):
        assert Path.cwd() == output / mode / "vnpy"
        assert (Path.cwd() / ".vntrader").is_dir()
        assert selected_mode == mode
        assert setting["柜台环境"] == ("实盘" if mode == "live" else "测试")
        assert setting["行情服务器"] == ""
        assert setting["密码"] == PASSWORD
        config_path.write_text("invalid TOML after startup")
        # VeighNa 后续写配置和日志的权限受本进程 umask 限制。
        report = Path.cwd() / ".vntrader" / "log.txt"
        report.write_text("no credentials")
        assert stat.S_IMODE(report.stat().st_mode) == 0o600
        calls.append(setting)
        return 0

    monkeypatch.setattr(assessment, "launch_gui", gui)
    assert assessment.main(["--mode", mode, "--output-dir", str(output)]) == 0
    assert Path.cwd() == previous_directory
    assert len(calls) == 1
    assert stat.S_IMODE((output / mode / "vnpy").stat().st_mode) == 0o700
    assert not list(output.rglob("connect_*.json"))
    console = capsys.readouterr()
    assert PASSWORD not in console.out + console.err


def install_gui_fakes(monkeypatch, *, fail=False):
    state = SimpleNamespace(closed=0, td_calls=[], full_calls=[], connects=[], apps=[])

    class Gateway:
        def __init__(self):
            self.td_api = SimpleNamespace(
                connect=lambda *values: state.td_calls.append(values)
            )

        def connect(self, setting):
            state.full_calls.append(setting)

        def init_query(self):
            state.query_started = True

    class Engine:
        def __init__(self, event_engine):
            state.engine = self

        def add_gateway(self, cls):
            state.gateway = cls()

        def add_app(self, cls):
            state.apps.append(cls)

        def connect(self, setting, name):
            state.connects.append((setting, name))
            state.gateway.connect(setting)

        def write_log(self, message):
            pass

        def close(self):
            state.closed += 1

    class Window:
        def __init__(self, engine, event_engine):
            state.window = self
            self.main_engine = engine

        def setWindowTitle(self, text):
            state.title = text

        def showMaximized(self):
            assert state.connects == []  # 打开窗口本身不连接交易服务。

    def event_loop():
        if fail:
            raise RuntimeError(PASSWORD)
        state.window.connect_gateway("CTP")  # 模拟用户明确点击连接。
        state.engine.close()  # 模拟标准 MainWindow 的退出清理。
        return 0

    settings = SimpleNamespace(
        Format=SimpleNamespace(IniFormat=1),
        Scope=SimpleNamespace(UserScope=1),
        setDefaultFormat=lambda *args: None,
        setPath=lambda *args: None,
    )
    exports = {
        "vnpy": {},
        "vnpy.trader": {},
        "vnpy.event": {"EventEngine": object},
        "vnpy.trader.engine": {"MainEngine": Engine},
        "vnpy.trader.ui": {
            "MainWindow": Window,
            "QtCore": SimpleNamespace(QSettings=settings),
            "create_qapp": lambda: SimpleNamespace(exec=event_loop),
        },
        "vnpy_ctp": {"CtpGateway": Gateway},
        "vnpy_riskmanager": {"RiskManagerApp": object},
    }
    for name, attributes in exports.items():
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)
    return state


@pytest.mark.parametrize("md_front", ["", "tcp://md.invalid:12346"])
def test_full_engine_connects_on_click_and_closes_once(monkeypatch, md_front):
    state = install_gui_fakes(monkeypatch)
    setting = assessment.gateway_setting(account(), md_front)
    assert assessment.launch_gui(setting, "sandbox") == 0
    assert state.closed == 1
    assert len(state.apps) == 1
    assert state.connects == [(setting, "CTP")]
    if md_front:
        assert state.full_calls == [setting]
        assert state.td_calls == []
    else:
        assert state.full_calls == []
        assert state.td_calls == [
            (
                setting["交易服务器"],
                ACCOUNT,
                PASSWORD,
                "9999",
                AUTH_CODE,
                "test-client",
                False,
            )
        ]
        assert state.query_started


def test_gui_failure_closes_engine_and_hides_credentials(tmp_path, monkeypatch, capsys):
    config = write_config(tmp_path)
    state = install_gui_fakes(monkeypatch, fail=True)
    assert (
        assessment.main(
            ["--config", str(config), "--output-dir", str(tmp_path / "gui")]
        )
        == 1
    )
    assert state.closed == 1
    assert PASSWORD not in capsys.readouterr().err


def test_just_isolates_packages_and_preserves_script_arguments(tmp_path):
    just = shutil.which("just")
    if just is None or os.name != "posix":
        pytest.skip("recipe check requires just and a POSIX shell")
    tools = tmp_path / "bin"
    tools.mkdir()
    capture = tmp_path / "arguments"
    (tools / "nix").write_text("#!/bin/sh\nprintf '%s\\n' /tmp\n")
    (tools / "uv").write_text(
        '#!/bin/sh\nprintf \'%s\\0\' "$@" > "$CTP_TEST_CAPTURE"\n'
    )
    for path in tools.iterdir():
        path.chmod(0o700)
    result = subprocess.run(
        [just, "ctp-assessment", "--config", "path with spaces.toml", "--mode", "live"],
        cwd=assessment.PROJECT_ROOT,
        env={
            **os.environ,
            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
            "CTP_TEST_CAPTURE": str(capture),
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    argv = capture.read_bytes().decode().rstrip("\0").split("\0")
    assert argv[:5] == ["run", "--no-project", "--no-config", "--isolated", "--python"]
    assert all(
        package in argv
        for package in ("vnpy==4.4.0", "vnpy_ctp==6.7.11.4", "vnpy_riskmanager==2.0.0")
    )
    assert argv[-4:] == ["--config", "path with spaces.toml", "--mode", "live"]


def test_cli_help_does_not_load_gui_or_backend(tmp_path):
    script = Path(assessment.__file__).resolve()
    command = """
import runpy, sys
sys.argv = [sys.argv[1], '--help']
try:
    runpy.run_path(sys.argv[0], run_name='__main__')
finally:
    assert not any(name in sys.modules for name in ('vnpy', 'vnpy_ctp', 'src.main', 'src.tools.ctp_native', 'src.tools.shared', 'tqsdk', 'ccxt'))
"""
    result = subprocess.run(
        [sys.executable, "-c", command, str(script)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "--md-front" in result.stdout
