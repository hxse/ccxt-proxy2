"""使用完整 VeighNa Trader 与期货公司联调；通过 just ctp-assessment 临时安装依赖。

默认复用 config.toml 的 ctp.test；窗口打开后点击“系统 → 连接CTP”。
加载官方 CTP 网关和风控模块，支持手动报撤单、查询及查看回报。
测试是否通过由期货公司确认。说明见 docs/ctp/02_assessment.md。
"""

import argparse
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tools.config_loader import ConfigError, load_config  # noqa: E402
from src.tools.config_types import CtpAccountConfig  # noqa: E402


def market_front(value: str) -> str:
    if not re.fullmatch(r"tcp://[^\s/:]+:[0-9]{1,5}", value):
        raise argparse.ArgumentTypeError("行情前置格式应为 tcp://主机:端口")
    if not 1 <= int(value.rsplit(":", 1)[1]) <= 65535:
        raise argparse.ArgumentTypeError("行情前置端口必须在 1~65535 之间")
    return value


def gateway_setting(account: CtpAccountConfig, md_front: str = "") -> dict[str, str]:
    # 官方网关将同一用户名用于 UserID 和 InvestorID，不能悄悄丢掉独立投资者编号。
    if account.user_id and account.user_id != account.investor_id:
        raise ConfigError(
            "官方 CTP 网关要求 user_id 与 investor_id 一致；请核对所选账号"
        )
    return {
        "用户名": account.investor_id,
        "密码": account.password.get_secret_value(),
        "经纪商代码": account.broker_id,
        "交易服务器": account.trader_front,
        "行情服务器": md_front,
        "产品名称": account.app_id or "",
        "授权编码": account.auth_code.get_secret_value() if account.auth_code else "",
        "柜台环境": "实盘" if account.production_mode else "测试",
    }


def launch_gui(setting: dict[str, str], mode: str) -> int:
    # 必须在准备好工作目录后再导入，防止 VeighNa 使用 ~/.vntrader 中的其他账号。
    # 这些模块由 just --with 提供，刻意不安装在后台的类型检查环境中。
    from vnpy.event import EventEngine  # ty: ignore[unresolved-import]
    from vnpy.trader.engine import MainEngine  # ty: ignore[unresolved-import]
    from vnpy.trader.ui import (  # ty: ignore[unresolved-import]
        MainWindow,
        QtCore,
        create_qapp,
    )
    from vnpy_ctp import CtpGateway
    from vnpy_riskmanager import RiskManagerApp  # ty: ignore[unresolved-import]

    class AssessmentGateway(CtpGateway):
        def connect(self, setting: dict) -> None:
            if setting["行情服务器"]:
                super().connect(setting)
            else:
                # 部分期货公司评测只给交易前置；复用官方交易 API，避免连接空行情地址。
                self.td_api.connect(
                    setting["交易服务器"],
                    setting["用户名"],
                    setting["密码"],
                    setting["经纪商代码"],
                    setting["授权编码"],
                    setting["产品名称"],
                    setting["柜台环境"] == "实盘",
                )
                self.init_query()

    class AssessmentWindow(MainWindow):
        def connect_gateway(self, gateway_name: str) -> None:
            # 沿用标准连接菜单，但从内存配置连接；不使用会保存密码 JSON 的 ConnectDialog。
            self.main_engine.connect(dict(setting), gateway_name)

    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.Format.IniFormat)
    QtCore.QSettings.setPath(
        QtCore.QSettings.Format.IniFormat,
        QtCore.QSettings.Scope.UserScope,
        str(Path.cwd() / ".vntrader" / "ui"),
    )
    app = create_qapp()
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    original_close = main_engine.close
    closed = False

    def close_once() -> None:
        # MainWindow.closeEvent 和 finally 都可能触发关闭，原生 CTP exit 只能调用一次。
        nonlocal closed
        if not closed:
            closed = True
            original_close()

    main_engine.close = close_once
    try:
        main_engine.add_gateway(AssessmentGateway)
        main_engine.add_app(RiskManagerApp)
        window = AssessmentWindow(main_engine, event_engine)
        window.setWindowTitle(
            f"VeighNa CTP 联调 | {mode} | 柜台环境：{setting['柜台环境']}"
        )
        window.showMaximized()
        main_engine.write_log(
            "已加载 TOML 账号；请点击 系统 → 连接CTP。风控规则请按期货公司清单设置。"
        )
        return app.exec()
    finally:
        main_engine.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path, help="省略时沿用 CCXT_PROXY_CONFIG_PATH/config.toml"
    )
    parser.add_argument(
        "--mode",
        choices=("sandbox", "live"),
        default="sandbox",
        help="默认 sandbox 读取 ctp.test；live 读取 ctp.live",
    )
    parser.add_argument(
        "--md-front",
        type=market_front,
        help="可选行情前置 tcp://主机:端口；省略时仅连接交易通道",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data/ctp_assessment",
        help="VeighNa 界面设置、风控配置、日志和 flow 的父目录",
    )
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        ctp = config.ctp
        account = (
            None if ctp is None else (ctp.test if args.mode == "sandbox" else ctp.live)
        )
        if account is None:
            raise ConfigError("请配置所选模式对应的 [ctp.test] 或 [ctp.live]")
        setting = gateway_setting(account, args.md_front or "")
        del account, ctp, config
        # 一个模式一个 VeighNa 工作目录，和 HTTP 后台的 flow、其他 VeighNa 程序隔离。
        runtime = (args.output_dir / args.mode / "vnpy").resolve()
        runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        (runtime / ".vntrader").mkdir(exist_ok=True, mode=0o700)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except OSError:
        print("无法创建 VeighNa 工作目录，请检查 --output-dir 的权限", file=sys.stderr)
        return 2

    previous_directory = Path.cwd()
    previous_umask = os.umask(0o077)
    try:
        os.chdir(runtime)
        return launch_gui(setting, args.mode)
    except ImportError as exc:
        print(
            f"VeighNa/Qt 依赖未就绪（{type(exc).__name__}）；请使用 just ctp-assessment 启动",
            file=sys.stderr,
        )
        return 2
    except Exception as exc:
        print(
            f"VeighNa 启动或运行失败（{type(exc).__name__}）；异常值已隐藏",
            file=sys.stderr,
        )
        return 1
    finally:
        os.chdir(previous_directory)
        os.umask(previous_umask)


if __name__ == "__main__":
    raise SystemExit(main())
