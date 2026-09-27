"""唯一部署入口：目标位置与显式动作组合，不补做未选择的步骤。"""

import argparse
import json
import sys
import tempfile
from contextlib import nullcontext
from pathlib import Path

from scripts.container_build import build_image
from scripts.container_common import (
    IMAGE,
    DeploymentError,
    inspect_image,
    install_signals,
    private_copy,
    project_lock,
    require_runtime,
)
from scripts.container_control import DeploymentCancelled, StartGuard
from scripts.container_local import activate, execute
from scripts.container_transport import remote_generation, request_remote

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv=None):
    values = sys.argv[1:] if argv is None else argv
    parser = argparse.ArgumentParser(
        description="按目标组合构建、上传、启动；控制操作独立执行"
    )
    parser.add_argument(
        "--target", choices=("local", "remote"), help="构建与运行的目标机器"
    )
    for action in ("build", "upload", "start", "stop", "status", "logs"):
        parser.add_argument("--" + action, action="store_true")
    parser.add_argument(
        "--config", type=Path, help="运行配置或 SSH 目标配置，默认 config.toml"
    )
    parser.add_argument(
        "--keep-remote-config",
        action="store_true",
        help="本次上传保留远端已准备的配置；默认完整覆盖上传两份配置",
    )
    if not values:
        parser.print_help()
        return None
    args = parser.parse_args(values)
    pipeline = args.build or args.upload or args.start
    controls = [name for name in ("stop", "status", "logs") if getattr(args, name)]
    if not args.target or not (pipeline or controls):
        parser.error("必须指定 --target 和至少一个动作")
    if len(controls) > 1 or (controls and pipeline):
        parser.error("--stop、--status、--logs 必须各自独立执行")
    if args.target == "local" and args.upload:
        parser.error("local 不支持 --upload")
    if args.keep_remote_config and not (args.target == "remote" and args.upload):
        parser.error("--keep-remote-config 只能与 --target=remote --upload 使用")
    if args.target == "remote" and args.upload and args.start and not args.build:
        parser.error("远程 --upload --start 必须显式包含 --build")
    args.control = controls[0] if controls else None
    args.remote = args.target == "remote" and bool(pipeline or controls)
    args.runtime_config = (args.upload and not args.keep_remote_config) or (
        args.target == "local" and args.start
    )
    if args.config is not None and not (args.remote or args.runtime_config):
        parser.error("当前动作不使用 --config")
    return args


def select_configuration(args):
    from src.tools.config_loader import (
        load_config,
        load_deployment_config,
        resolve_config_path,
    )

    if not (args.remote or args.runtime_config):
        return None, None
    selected = args.config or resolve_config_path()
    path = selected if selected.is_absolute() else ROOT / selected
    if args.runtime_config:
        config = load_config(path)
        from src.tools.market_data_config import load_market_data_plan, validate_client

        validate_client(load_market_data_plan(ROOT / "market_data.toml"), config)
        target = config.deployment
        if args.remote and target is None:
            raise DeploymentError("请配置 [deployment] 的 ssh_host 和 remote_dir")
    else:
        target = load_deployment_config(path)
    return path, target


def run_pipeline(args, config_path, target):
    generation = remote_generation(target) if args.remote and args.start else None
    if args.remote:
        action = "-".join(
            name for name in ("upload", "build", "start") if getattr(args, name)
        )
        with project_lock(ROOT) if args.upload else nullcontext():
            if args.runtime_config:
                with tempfile.TemporaryDirectory(
                    prefix="ccxt-proxy2-config-"
                ) as directory:
                    source = Path(directory)
                    assert config_path is not None
                    private_copy(config_path, source / "config.toml")
                    private_copy(ROOT / "market_data.toml", source / "market_data.toml")
                    request_remote(target, action, source=source, generation=generation)
            else:
                request_remote(
                    target,
                    action,
                    generation=generation,
                    keep_remote_config=args.keep_remote_config,
                )
        return
    guard = StartGuard(ROOT) if args.target == "local" and args.start else None
    with project_lock(ROOT, check_cancel=guard.check if guard else None):
        require_runtime()
        if args.build:
            build_image()
        if args.start:
            image = inspect_image(IMAGE)["Id"]
            with tempfile.TemporaryDirectory(prefix="ccxt-proxy2-config-") as directory:
                source = Path(directory)
                assert config_path is not None
                private_copy(config_path, source / "config.toml")
                private_copy(ROOT / "market_data.toml", source / "market_data.toml")
                activate(ROOT, image, source, guard=guard)


def main(argv=None):
    args = parse_args(argv)
    if args is None:
        return 0
    install_signals()
    from src.tools.config_loader import ConfigError

    try:
        config_path, target = select_configuration(args)
        if args.control:
            if args.remote:
                request_remote(target, args.control)
            else:
                result = execute(ROOT, args.control)
                if result is not None:
                    print(json.dumps(result), flush=True)
        else:
            run_pipeline(args, config_path, target)
    except DeploymentCancelled as exc:
        print(str(exc), file=sys.stderr)
        return 130
    except (DeploymentError, ConfigError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except OSError:
        print(
            "无法读取或保存部署文件，请检查路径和权限；配置值已隐藏。", file=sys.stderr
        )
        return 1
    except KeyboardInterrupt:
        print("操作已取消。", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
