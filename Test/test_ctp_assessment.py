"""联调脚本离线验证：只使用假 API，不连接账户、不调用原生采集。"""

import io
import json
import os
import stat
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

from script import ctp_assessment as assessment
from src.tools.config_types import CtpAccountConfig

PASSWORD = "offline-password"
AUTH_CODE = "offline-authcode"
ACCOUNT = "123456789"
LOGIN = {
    "FrontID": 1,
    "SessionID": 2,
    "TradingDay": "20260918",
    "LoginTime": "21:01:02",
}


def fake_api(*, failure=None, return_code=0):
    calls = []

    class Api:
        onFrontConnected: Callable[[], None]
        onFrontDisconnected: Callable[[int], None]
        onRspAuthenticate: Callable[[dict, dict, int, bool], None]
        onRspUserLogin: Callable[[dict, dict, int, bool], None]
        onRspError: Callable[[dict, int, bool], None]

        def createFtdcTraderApi(self, path, production_mode):
            calls.append(("create", path, production_mode))

        def registerFront(self, front):
            calls.append(("front", front))

        def getApiVersion(self):
            return "offline-api-version"

        def init(self):
            calls.append(("init",))
            if failure != "connect_timeout":
                self.onFrontConnected()

        def reqAuthenticate(self, fields, request_id):
            calls.append(("auth", fields, request_id))
            if failure == "authenticate":
                self.onRspAuthenticate(
                    {},
                    {
                        "ErrorID": 63,
                        "ErrorMsg": f"认证失败 {PASSWORD} {AUTH_CODE} {ACCOUNT}",
                    },
                    request_id,
                    True,
                )
                self.onFrontConnected()  # 断言脚本不随重连再次认证。
            elif failure == "disconnect":
                self.onFrontDisconnected(4097)
            elif failure != "auth_timeout":
                # 错误的 request ID 不应完成当前请求，也不应让当前请求失败。
                self.onRspAuthenticate({}, {"ErrorID": 63}, 999, True)
                self.onRspError({"ErrorID": 63}, 999, True)
                self.onRspAuthenticate({}, {}, request_id, False)
                self.onRspAuthenticate({}, {}, request_id, True)
            return return_code

        def reqUserLogin(self, fields, request_id):
            calls.append(("login", fields, request_id))
            if failure == "login":
                self.onRspUserLogin(
                    {}, {"ErrorID": 3, "ErrorMsg": "不合法的登录"}, request_id, True
                )
            elif failure == "rsp_error":
                self.onRspError(
                    {"ErrorID": 7, "ErrorMsg": "尚未初始化"}, request_id, True
                )
            elif failure == "empty_login":
                self.onRspUserLogin({}, {}, request_id, True)
            elif failure != "login_timeout":
                self.onRspUserLogin(
                    {**LOGIN, "UserID": ACCOUNT, "Password": PASSWORD},
                    {},
                    request_id,
                    True,
                )
                if failure == "observe_disconnect":
                    self.onFrontDisconnected(4097)
            return 0

        def exit(self):
            calls.append(("exit",))

    return Api, calls


def make_probe():
    stream = io.StringIO()
    account = CtpAccountConfig(
        trader_front="tcp://assessment.invalid:12345",
        broker_id="9999",
        investor_id=ACCOUNT,
        user_id="987654321",
        password=PASSWORD,
        app_id="test-client",
        auth_code=AUTH_CODE,
        production_mode=False,
    )
    return assessment.Probe(account, stream), stream


def records(stream):
    return [json.loads(line) for line in stream.getvalue().splitlines()]


def test_probe_only_authenticates_and_logs_in_with_original_config(tmp_path, capsys):
    probe, stream = make_probe()
    api, calls = fake_api()
    probe.run(api, tmp_path, 0.01, 0.01, 0)
    assert [call[0] for call in calls] == [
        "create",
        "front",
        "init",
        "auth",
        "login",
        "exit",
    ]
    assert calls[0] == ("create", str(tmp_path.resolve()) + "/", False)
    assert calls[3][1] == {
        "BrokerID": "9999",
        "UserID": "987654321",
        "AppID": "test-client",
        "AuthCode": AUTH_CODE,
    }
    assert calls[4][1] == {
        "BrokerID": "9999",
        "UserID": "987654321",
        "Password": PASSWORD,
    }
    assert probe.login_ok is True
    assert (
        next(row for row in records(stream) if row["event"] == "login")["SessionID"]
        == 2
    )
    output = stream.getvalue() + capsys.readouterr().out
    assert all(
        secret not in output for secret in (PASSWORD, AUTH_CODE, ACCOUNT, "987654321")
    )


@pytest.mark.parametrize(
    "failure",
    [
        "authenticate",
        "disconnect",
        "connect_timeout",
        "auth_timeout",
        "login_timeout",
        "login",
        "rsp_error",
        "empty_login",
        "observe_disconnect",
    ],
)
def test_failures_are_bounded_and_cleanup_without_retry(tmp_path, capsys, failure):
    probe, stream = make_probe()
    api, calls = fake_api(failure=failure)
    with pytest.raises(assessment.ProbeError):
        probe.run(api, tmp_path, 0.005, 0.005, 0)
    assert calls[-1] == ("exit",)
    assert sum(call[0] == "auth" for call in calls) <= 1
    assert sum(call[0] == "login" for call in calls) <= 1
    if failure in ("authenticate", "disconnect", "connect_timeout", "auth_timeout"):
        assert not any(call[0] == "login" for call in calls)
    assert probe.login_ok is (failure == "observe_disconnect")
    output = stream.getvalue() + capsys.readouterr().out
    assert all(secret not in output for secret in (PASSWORD, AUTH_CODE, ACCOUNT))
    if failure == "authenticate":
        assert "认证失败" in output and '"error_id": 63' in output
    if failure in ("disconnect", "observe_disconnect"):
        assert '"reason": 4097' in output


def test_immediate_send_failure_is_not_mistaken_for_callback_success(tmp_path):
    probe, stream = make_probe()
    api, calls = fake_api(return_code=-2)
    with pytest.raises(assessment.ProbeError, match="未成功发送"):
        probe.run(api, tmp_path, 0.01, 0.01, 0)
    assert not probe.login_ok
    assert not any(call[0] == "login" for call in calls)
    assert (
        next(row for row in records(stream) if row["event"] == "request_sent")[
            "return_code"
        ]
        == -2
    )


def write_config(tmp_path):
    path = tmp_path / "private.toml"
    path.write_text(
        f'''SECRET = "offline-assessment-secret"
[ctp]
connect_timeout_seconds = 0.01
request_timeout_seconds = 0.01
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
@pytest.mark.parametrize(
    "failure,exit_code", [(None, 0), ("authenticate", 1), ("observe_disconnect", 1)]
)
def test_cli_writes_private_report_with_unknown_collection_verdict(
    tmp_path, monkeypatch, capsys, mode, failure, exit_code
):
    config_path = write_config(tmp_path)
    api, calls = fake_api(failure=failure)
    monkeypatch.setattr(assessment, "load_td_api", lambda: api)
    monkeypatch.setattr(assessment, "version", lambda name: "offline-vnpy-ctp")
    output = tmp_path / "reports"
    assert (
        assessment.main(
            [
                "--config",
                str(config_path),
                "--mode",
                mode,
                "--hold-seconds",
                "0",
                "--output-dir",
                str(output),
            ]
        )
        == exit_code
    )
    reports = list(output.glob("*/report.jsonl"))
    assert len(reports) == 1
    text = reports[0].read_text()
    rows = [json.loads(line) for line in text.splitlines()]
    assert rows[0]["mode"] == mode
    assert rows[0]["production_mode"] is (mode == "live")
    assert rows[0]["connect_timeout_seconds"] == 0.01
    assert rows[-1]["collection_verified"] is None
    assert rows[-1]["status"] == ("login_ok" if exit_code == 0 else "failed")
    assert rows[-1]["login_ok"] is (failure != "authenticate")
    assert stat.S_IMODE(reports[0].stat().st_mode) == 0o600
    assert stat.S_IMODE(reports[0].parent.stat().st_mode) == 0o700
    assert Path(calls[0][1]) == reports[0].parent / "flow"
    console = capsys.readouterr()
    assert all(
        secret not in text + console.out + console.err
        for secret in (PASSWORD, AUTH_CODE, ACCOUNT)
    )


def test_invalid_config_fails_before_loading_native_sdk(tmp_path, monkeypatch, capsys):
    path = tmp_path / "invalid.toml"
    path.write_text(f"password = '{PASSWORD}'\n")
    monkeypatch.setattr(
        assessment, "load_td_api", lambda: pytest.fail("must not load SDK")
    )
    assert assessment.main(["--config", str(path)]) == 2
    assert PASSWORD not in capsys.readouterr().err


def test_cli_help_works_without_config_and_does_not_start_application(tmp_path):
    script = Path(assessment.__file__).resolve()
    command = "import runpy, sys; sys.argv = [sys.argv[1], '--help']; "
    command += "\ntry: runpy.run_path(sys.argv[0], run_name='__main__')\nfinally:\n"
    command += " assert not any(name in sys.modules for name in ('vnpy', 'vnpy_ctp', 'src.main', 'src.tools.shared', 'tqsdk', 'ccxt'))"
    result = subprocess.run(
        [sys.executable, "-c", command, str(script)],
        cwd=tmp_path,
        env={**os.environ, "CCXT_PROXY_CONFIG_PATH": str(tmp_path / "missing.toml")},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "--mode" in result.stdout and "production_mode" in result.stdout
