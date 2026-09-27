"""本地和远端共用的 Podman 操作；只依赖 Python 标准库。"""

import fcntl
import json
import os
import platform
import signal
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

IMAGE = "localhost/ccxt-proxy2:latest"
NAME = "ccxt-proxy2"
PREFIX = "io.ccxt-proxy2."
CONFIG_FILES = ("config.toml", "market_data.toml")


class DeploymentError(RuntimeError):
    """可公开的部署诊断，不包含配置值。"""


def command(args, *, capture=True, timeout=60, stdin=None, merge_stderr=False):
    try:
        with subprocess.Popen(
            [str(value) for value in args],
            stdin=stdin,
            stdout=subprocess.PIPE if capture else None,
            stderr=(subprocess.STDOUT if merge_stderr else subprocess.PIPE)
            if capture
            else None,
            text=True,
        ) as process:
            try:
                stdout, _ = process.communicate(timeout=timeout)
            except BaseException:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise
            if process.returncode == 130:
                raise KeyboardInterrupt
            if process.returncode:
                raise DeploymentError(
                    f"命令失败：{args[0]} {args[1]}，退出码 {process.returncode}"
                )
            return stdout or ""
    except subprocess.TimeoutExpired:
        raise DeploymentError(f"命令超时：{args[0]} {args[1]}") from None
    except OSError:
        raise DeploymentError(f"无法执行 {args[0]}，请检查安装与权限") from None


def podman(*args, **kwargs):
    return command(["podman", *args], **kwargs)


def require_runtime():
    if platform.system() != "Linux" or platform.machine() not in {"x86_64", "amd64"}:
        raise DeploymentError("容器部署只支持 Linux x86_64")
    if podman("info", "--format", "{{.Host.Security.Rootless}}").strip() != "true":
        raise DeploymentError("请使用普通登录用户的 rootless Podman")


def install_signals():
    def cancel(signum, frame):
        raise KeyboardInterrupt

    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, cancel)


@contextmanager
def file_lock(path: Path, *, announce=False, check_cancel=None):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("a") as stream:
        waiting = False
        while True:
            if check_cancel is not None:
                check_cancel()
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if announce and not waiting:
                    print(
                        "另一个容器任务正在执行，等待项目锁；可按 Ctrl+C 取消。",
                        flush=True,
                    )
                    waiting = True
                time.sleep(0.2)
        try:
            if check_cancel is not None:
                check_cancel()
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def project_lock(root: Path, *, check_cancel=None):
    return file_lock(
        root / ".container/operation.lock", announce=True, check_cancel=check_cancel
    )


def labels(info: dict[str, Any]) -> dict[str, str]:
    return info.get("Labels") or info.get("Config", {}).get("Labels") or {}


def inspect_image(reference: str) -> dict[str, Any]:
    try:
        info = json.loads(podman("image", "inspect", reference))[0]
    except (DeploymentError, ValueError, IndexError):
        raise DeploymentError(
            "镜像不存在或不可读取，请先执行 just deploy --target=local --build"
        ) from None
    if (
        info.get("Os") != "linux"
        or info.get("Architecture") != "amd64"
        or labels(info).get(PREFIX + "kind") != "runtime"
        or labels(info).get(PREFIX + "release") != "true"
        or labels(info).get(PREFIX + "project") != NAME
    ):
        raise DeploymentError("镜像必须是本项目的 linux/amd64 运行镜像")
    return info


def read_prepared(root: Path) -> dict[str, str] | None:
    try:
        parts = (root / ".container/prepared").read_text().splitlines()
        if len(parts) != 2:
            raise ValueError
        result = dict(zip(("image_id", "configuration"), parts))
    except FileNotFoundError:
        return None
    except (OSError, ValueError):
        raise DeploymentError("准备版本元数据不可读取") from None
    if not isinstance(result, dict) or set(result) != {"image_id", "configuration"}:
        raise DeploymentError("准备版本元数据无效")
    for key, value in result.items():
        normalized = (
            value.removeprefix("sha256:")
            if key == "image_id" and isinstance(value, str)
            else value
        )
        if (
            not isinstance(normalized, str)
            or len(normalized) != 64
            or any(char not in "0123456789abcdef" for char in normalized)
        ):
            raise DeploymentError("准备版本元数据无效")
    return result


def private_copy(source: Path, target: Path):
    write_private(target, source.read_bytes())


def write_private(target: Path, content: bytes):
    temporary = target.with_name(target.name + ".upload")
    # 不沿用已有临时文件的宽松权限，也不跟随符号链接。
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)


VALIDATE_CONFIG = Path(__file__).with_name("container_validate.py").read_text()
