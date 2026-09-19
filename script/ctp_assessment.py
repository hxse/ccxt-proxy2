"""期货公司 CTP 穿透式采集联调：连接 → 认证 → 登录 → 保持连接。

复用项目 TOML 和 vnpy_ctp 交易扩展；不启动 HTTP/TQ/CCXT，不确认结算、不下单。
默认读取 [ctp.test]；期货公司评测通常 production_mode=false，SimNow 为 true。
登录成功不代表采集验收通过，需期货公司核对后台记录。用法见 docs/ctp/02_assessment.md。
"""

import argparse
import json
import os
import platform
import sys
import time
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from queue import Empty, Queue
from tempfile import mkdtemp
from typing import Any, TextIO

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tools.config_loader import ConfigError, load_config  # noqa: E402
from src.tools.config_types import CtpAccountConfig  # noqa: E402
from src.tools.ctp_native import load_td_api  # noqa: E402


class ProbeError(RuntimeError):
    """可直接写入报告的诊断信息，不包含请求中的凭证。"""


class Probe:
    def __init__(self, account: CtpAccountConfig, stream: TextIO):
        self.account = account
        self.stream = stream
        self.events: Queue = Queue()
        self.started_at = time.monotonic()
        self.stage = "initialize"
        self.login_ok = False

    def log(self, event: str, **fields: Any) -> None:
        record = {
            "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "elapsed_ms": round((time.monotonic() - self.started_at) * 1000),
            "stage": self.stage,
            "event": event,
            **fields,
        }
        line = json.dumps(record, ensure_ascii=False)
        self.stream.write(line + "\n")
        self.stream.flush()
        print(line, flush=True)

    def error_message(self, error: dict) -> str:
        message = str(error.get("ErrorMsg", "")).rstrip("\x00")
        # 柜台错误有时会回显请求内容；报告不保留密码、授权码或完整账号。
        hidden = [self.account.investor_id, self.account.user_id or ""]
        for secret in (self.account.password, self.account.auth_code):
            if secret is not None:
                hidden.append(secret.get_secret_value())
        for value in sorted(filter(None, hidden), key=len, reverse=True):
            message = message.replace(value, "***")
        return message

    def wait(self, expected: str | None, timeout: float, request_id: int = 0) -> dict:
        """只等待当前请求；断线即结束，不自动重新认证或登录。"""
        deadline = time.monotonic() + timeout
        while True:
            try:
                name, data, error, reqid, last = self.events.get(
                    timeout=max(0, deadline - time.monotonic())
                )
            except Empty:
                if expected is None:  # 登录后观察窗口正常结束。
                    return {}
                raise ProbeError(f"等待 {expected} 超时") from None
            if name == "onFrontDisconnected":
                self.log(name, reason=data["reason"], reason_hex=hex(data["reason"]))
                raise ProbeError("交易前置断线，请核对网络、前置地址和 production_mode")
            if (name == "onRspError" and reqid in (0, request_id)) or (
                name == expected and reqid == request_id
            ):
                self.log(
                    name,
                    request_id=reqid,
                    is_last=last,
                    error_id=error.get("ErrorID", 0),
                    error_msg=self.error_message(error),
                )
                if error.get("ErrorID", 0):
                    raise ProbeError("CTP 返回错误，详见上一条回报")
                if name == expected and last:
                    return data
            else:
                self.log("ignored_callback", callback=name, request_id=reqid)
            if time.monotonic() >= deadline:
                if expected is None:
                    return {}
                raise ProbeError(f"等待 {expected} 超时")

    def request(self, api: Any, method: str, fields: dict, reqid: int, timeout: float):
        self.stage = method
        code = getattr(api, method)(fields, reqid)
        self.log("request_sent", method=method, request_id=reqid, return_code=code)
        if code != 0:
            raise ProbeError("CTP 请求未成功发送；未重试")
        return self.wait("onRsp" + method[3:], timeout, reqid)

    def run(
        self,
        td_api: Any,
        flow: Path,
        connect_timeout: float,
        request_timeout: float,
        hold_seconds: int,
    ) -> None:
        # 回调仅入队，认证/登录由主线程顺序发送，避免重连时重复使用密码。
        events = self.events

        class Trader(td_api):
            def onFrontConnected(self):
                events.put(("onFrontConnected", {}, {}, 0, True))

            def onFrontDisconnected(self, reason):
                events.put(("onFrontDisconnected", {"reason": reason}, {}, 0, True))

            def onRspAuthenticate(self, data, error, reqid, last):
                events.put(("onRspAuthenticate", dict(data), dict(error), reqid, last))

            def onRspUserLogin(self, data, error, reqid, last):
                events.put(("onRspUserLogin", dict(data), dict(error), reqid, last))

            def onRspError(self, error, reqid, last):
                events.put(("onRspError", {}, dict(error), reqid, last))

        api = Trader()
        created = initialized = False
        try:
            api.createFtdcTraderApi(
                str(flow.resolve()) + "/", self.account.production_mode
            )
            created = True
            self.log("sdk", api_version=api.getApiVersion())
            api.registerFront(self.account.trader_front)
            self.stage = "connect"
            initialized = True
            api.init()
            self.wait("onFrontConnected", connect_timeout)
            identity = {
                "BrokerID": self.account.broker_id,
                "UserID": self.account.user_id or self.account.investor_id,
            }
            if self.account.auth_code is None or self.account.app_id is None:
                raise ProbeError("穿透式联调需要配置 app_id 和 auth_code")
            self.request(
                api,
                "reqAuthenticate",
                {
                    **identity,
                    "AppID": self.account.app_id,
                    "AuthCode": self.account.auth_code.get_secret_value(),
                },
                1,
                request_timeout,
            )
            login = self.request(
                api,
                "reqUserLogin",
                {**identity, "Password": self.account.password.get_secret_value()},
                2,
                request_timeout,
            )
            required = {"FrontID", "SessionID", "TradingDay", "LoginTime"}
            if not required <= login.keys():
                raise ProbeError("登录回报缺少会话字段，不能确认登录成功")
            self.login_ok = True
            self.log("login", **{key: login[key] for key in sorted(required)})
            self.stage = "observe"
            self.log(
                "holding",
                seconds=hold_seconds,
                message="请期货公司核对本次终端采集记录",
            )
            self.wait(None, hold_seconds)
        finally:
            if created:
                # VeighNa exit 会 join 回调线程，必须先 init；与项目会话释放方式一致。
                if not initialized:
                    api.init()
                api.exit()
                self.log("closed")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        help="TOML 文件；省略时沿用 CCXT_PROXY_CONFIG_PATH/config.toml",
    )
    parser.add_argument(
        "--mode",
        choices=("sandbox", "live"),
        default="sandbox",
        help="选择 ctp.test 或 ctp.live；默认 sandbox，不覆盖 production_mode",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        help="覆盖连接和每次请求的等待秒数（1~300）；省略沿用 TOML",
    )
    parser.add_argument(
        "--hold-seconds",
        type=int,
        default=30,
        help="登录后保持连接秒数（0~3600），默认 30",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data/ctp_assessment",
        help="报告和独立 flow 目录的父目录，默认 data/ctp_assessment",
    )
    args = parser.parse_args(argv)
    if args.timeout is not None and not 1 <= args.timeout <= 300:
        parser.error("--timeout 必须在 1~300 秒之间")
    if not 0 <= args.hold_seconds <= 3600:
        parser.error("--hold-seconds 必须在 0~3600 秒之间")
    try:
        config = load_config(args.config)
        ctp = config.ctp
        account = (
            None if ctp is None else (ctp.test if args.mode == "sandbox" else ctp.live)
        )
        if ctp is None or account is None:
            raise ConfigError("请配置所选模式对应的 [ctp.test] 或 [ctp.live]")
        if account.app_id is None or account.auth_code is None:
            raise ConfigError("穿透式联调需要配置 app_id 和 auth_code")
        # 手动联调只运行选中的 CTP 账号；白名单仍只控制后台服务的自动初始化。
        connect_timeout = args.timeout or ctp.connect_timeout_seconds
        request_timeout = args.timeout or ctp.request_timeout_seconds
        del config, ctp
        td_api = load_td_api()
        package_version = version("vnpy_ctp")
        args.output_dir.mkdir(parents=True, exist_ok=True)
        run_dir = Path(
            mkdtemp(
                prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_"),
                dir=args.output_dir,
            )
        )
        flow = run_dir / "flow"
        flow.mkdir(mode=0o700)
        report_path = run_dir / "report.jsonl"
        fd = os.open(report_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(
            f"准备失败（{type(exc).__name__}），请检查 ctp 依赖和输出目录权限；安装命令：uv sync --locked --extra ctp",
            file=sys.stderr,
        )
        return 2
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        probe = Probe(account, stream)
        probe.log(
            "start",
            mode=args.mode,
            production_mode=account.production_mode,
            trader_front=account.trader_front,
            broker_id=account.broker_id,
            app_id=account.app_id,
            account="***" + account.investor_id[-4:]
            if len(account.investor_id) > 4
            else "***",
            vnpy_ctp_version=package_version,
            python_version=platform.python_version(),
            system=platform.system(),
            architecture=platform.machine(),
            connect_timeout_seconds=connect_timeout,
            request_timeout_seconds=request_timeout,
        )
        code, status, message = (
            0,
            "login_ok",
            "连接、认证和登录成功；采集是否合格请期货公司确认",
        )
        try:
            probe.run(td_api, flow, connect_timeout, request_timeout, args.hold_seconds)
        except KeyboardInterrupt:
            code, status, message = 130, "interrupted", "用户中断测试"
        except ProbeError as exc:
            code, status, message = 1, "failed", str(exc)
        except Exception as exc:
            code, status, message = (
                1,
                "failed",
                f"SDK 或本地调用异常（{type(exc).__name__}），异常值已隐藏",
            )
        probe.log(
            "result",
            status=status,
            login_ok=probe.login_ok,
            collection_verified=None,
            message=message,
        )
    print(f"联调报告：{report_path}", file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
