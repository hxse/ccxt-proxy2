"""CFB 运维入口；控制复用 socket CLI，日志读取所选容器的受管文件。"""

import argparse
import base64
import json
import shlex
from pathlib import Path

from scripts.container_common import DeploymentError, command
from src.base_types import parse_is_live
from src.tools.config_loader import ConfigError, load_deployment_config

ROOT = Path(__file__).resolve().parents[1]


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="CFB 独立执行器运维")
    parser.add_argument("--target", choices=["local", "remote"], default="local")
    parser.add_argument("--is-live", type=parse_is_live, required=True, metavar="true|false")
    actions = parser.add_mutually_exclusive_group(required=True)
    for action in ("status", "logs", "pause", "resume", "clean", "screenshot"):
        actions.add_argument("--" + action, action="store_const", dest="action", const=action)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    mode = "live" if args.is_live else "sandbox"
    name = f"ccxt-proxy2-cfb-{mode}"
    output = args.output or Path(f"debug/cfb-{mode}-desktop.png")
    target = None

    def invoke(values, *, capture=True, timeout=60):
        if args.target == "remote":
            assert target is not None
            values = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                      target.ssh_host, shlex.join([str(value) for value in values])]
        return command(values, capture=capture, timeout=timeout)

    try:
        directory = str(ROOT)
        if args.target == "remote":
            target = load_deployment_config(profile="remote")
            directory = invoke(["sh", "-c", 'cd -- "$HOME/$1" && pwd -P', "sh", target.remote_dir]).strip()
        label = invoke(["podman", "inspect", "--format", '{{index .Config.Labels "io.ccxt-proxy2.component"}}|{{index .Config.Labels "io.ccxt-proxy2.directory"}}|{{index .Config.Labels "io.ccxt-proxy2.mode"}}', name])
        if label.strip() != f"cfb|{directory}|{mode}":
            raise DeploymentError("CFB 容器不是本项目受管实例")
        if args.action == "logs":
            invoke(["podman", "exec", name, "/app/.venv/bin/python", "-m", "src.cfb.log_reader"],
                   capture=False, timeout=None)
            return 0
        result = invoke(["podman", "exec", name, "/app/.venv/bin/python", "-m", "src.cfb.ctl", args.action])
        if args.action == "screenshot":
            payload = json.loads(result)
            data = base64.b64decode(payload["base64"], validate=True)
            if not data.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError("invalid PNG")
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(data)
            print(output)
        else:
            print(result.strip())
        return 0
    except KeyboardInterrupt:
        return 130
    except (ConfigError, DeploymentError, OSError, ValueError, KeyError):
        print("CFB 运维失败，请检查受管容器状态；不会切换到原项目实例。")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
