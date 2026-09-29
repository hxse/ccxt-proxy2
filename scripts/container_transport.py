"""SSH 请求封装：上传构建源码和配置，远端通过 Podman 构建。"""

import json
import shlex
import tarfile
import tempfile
from pathlib import Path

from scripts.container_common import (
    CONFIG_FILES,
    DeploymentError,
    command,
    private_copy,
)
from scripts.container_source import parse_inventory, prepare_source

ROOT = Path(__file__).resolve().parents[1]
CONTROL_FILES = {
    "scripts/container_env.sh",
    "scripts/container_instance.sh",
    "scripts/container_manage.sh",
    "scripts/container_validate.py",
    "scripts/container_source.sh",
    "scripts/container_manifest.sh",
    "scripts/container_build.sh",
    "scripts/container_cleanup.sh",
    "scripts/container_smoke.py",
    "scripts/container_cfb.sh",
}
UPLOAD_FILES = {
    "source.delta.tar.gz",
    "source.manifest",
    "source.base",
    "config.toml",
    "market_data.toml",
}
TIMEOUTS = {
    "generation": 30,
    "inventory": 60,
    "status": 30,
    "stop": 120,
    "start": 240,
    "upload": 900,
    "build": None,
    "build-start": None,
    "upload-build": None,
    "upload-build-start": None,
    "logs": None,
}


def request_remote(
    target,
    action,
    *,
    source=None,
    generation=None,
    capture=False,
    keep_remote_config=False,
):
    if action not in TIMEOUTS:
        raise DeploymentError("不支持的远程动作")
    if "start" in action and generation is None:
        raise DeploymentError("启动前必须固定远端停止代次")
    if keep_remote_config and (not action.startswith("upload") or source):
        raise DeploymentError("保留远端配置仅用于上传，不能同时提供本地运行配置")
    with tempfile.TemporaryDirectory(prefix="ccxt-proxy2-upload-") as directory:
        workspace = Path(directory)
        files = set(CONTROL_FILES)
        if action.startswith("upload"):
            files.update(UPLOAD_FILES)
            if keep_remote_config:
                files.difference_update(CONFIG_FILES)
            else:
                if source is None:
                    raise DeploymentError("缺少上传配置")
                private_copy(source / "config.toml", workspace / "config.toml")
                private_copy(
                    source / "market_data.toml", workspace / "market_data.toml"
                )
            inventory = parse_inventory(
                request_remote(target, "inventory", capture=True)
            )
            summary = prepare_source(ROOT, workspace, inventory)
            print(
                f"源码增量上传：{summary['changed']}/{summary['total']} 个文件，"
                f"{summary['bytes']} 字节；配置按本次选项完整处理。",
                flush=True,
            )
        bundle = workspace / "bundle.tar.gz"
        with tarfile.open(bundle, "w:gz", compresslevel=1) as archive:
            for name in sorted(files):
                origin = (
                    ROOT / name if name.startswith("scripts/") else workspace / name
                )
                archive.add(origin, arcname=name, recursive=False)
        receiver = (ROOT / "scripts/container_receive.sh").read_text()
        remote_command = shlex.join(
            [
                "sh",
                "-c",
                receiver,
                "ccxt-proxy2-receive",
                target.remote_dir,
                action,
                str(generation) if generation is not None else "",
                "true" if keep_remote_config else "false",
            ]
        )
        with bundle.open("rb") as stream:
            return command(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    "ConnectTimeout=10",
                    "-o",
                    "ServerAliveInterval=15",
                    "-o",
                    "ServerAliveCountMax=2",
                    target.ssh_host,
                    remote_command,
                ],
                capture=capture,
                stdin=stream,
                timeout=TIMEOUTS[action],
            )


def remote_generation(target):
    try:
        value = json.loads(request_remote(target, "generation", capture=True))[
            "generation"
        ]
        if type(value) is not int or value < 0:
            raise ValueError
        return value
    except (ValueError, KeyError, TypeError):
        raise DeploymentError("无法取得有效的远端停止代次") from None
