"""宿主源码启动入口；只选配置和监听参数，不安装或同步依赖。"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def port_number(value):
    try:
        port = int(value)
        if not 1 <= port <= 65535:
            raise ValueError
        return port
    except ValueError:
        raise argparse.ArgumentTypeError("port 必须是 1..65535 的整数") from None


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="本地源码开发服务；依赖请先通过 just sync 准备"
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=port_number, default=5123)
    args = parser.parse_args(argv)
    if not args.host.strip():
        parser.error("host 不得为空")
    from src.tools.config_loader import (
        CONFIG_PATH_VARIABLE,
        ConfigError,
        load_config,
        resolve_config_path,
    )

    try:
        selected = args.config or resolve_config_path()
        selected = selected if selected.is_absolute() else ROOT / selected
        load_config(selected)
        os.environ[CONFIG_PATH_VARIABLE] = str(selected.resolve())
        os.execvp(
            "uvicorn",
            [
                "uvicorn",
                "src.main:app",
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--reload",
            ],
        )
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except OSError:
        print("无法启动 uvicorn，请先执行 just sync，并检查运行环境。", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
