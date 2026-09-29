"""冻结白名单源码，以完整清单和内容差异上传；配置始终独立传输。"""

import gzip
import hashlib
import io
import re
import tarfile
from dataclasses import dataclass
from pathlib import Path

from scripts.container_common import DeploymentError

BUILD_FILES = (
    "Dockerfile",
    ".dockerignore",
    "pyproject.toml",
    "uv.lock",
    "scripts/collect_market_data.py",
    "scripts/prune_market_data.py",
    "scripts/market_data_pipeline.py",
    "src/cfb/terminal.lock.toml",
    "containers/cfb/Containerfile",
    "containers/cfb/pyproject.toml",
    "containers/cfb/uv.lock",
)


@dataclass(frozen=True)
class SourceInventory:
    identity: str
    hashes: dict[str, str]


def allowed_path(name: str) -> bool:
    if not re.fullmatch(r"[A-Za-z0-9_./+-]+", name) or any(
        part in {"", ".", "..", "__pycache__"} for part in name.split("/")
    ):
        return False
    return (
        name in BUILD_FILES
        or (name.startswith("src/") and name.endswith(".py"))
        or (name.startswith("src/openapi/") and name.endswith(".json"))
        or (name.startswith("containers/cfb/native/") and name.endswith((".c", ".h")))
        or (name.startswith("containers/cfb/container/") and name.endswith((".py", ".c", ".reg", ".xml")))
        or (
            name.startswith("vendor/vnpy_ctp/")
            and len(name.split("/")) == 3
            and (
                name.endswith((".tar.gz", ".patch"))
                or name.rsplit("/", 1)[1]
                in {"README.md", "upstream.json", "SHA256SUMS"}
            )
        )
    )


def parse_inventory(raw: str) -> SourceInventory:
    lines = raw.splitlines()
    if not lines or not re.fullmatch(r"none|[0-9a-f]{64}", lines[0]):
        raise DeploymentError("远端源码清单身份无效")
    hashes = {}
    for line in lines[1:]:
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None or not allowed_path(match[2]) or match[2] in hashes:
            raise DeploymentError("远端源码清单路径或摘要无效")
        hashes[match[2]] = match[1]
    return SourceInventory(lines[0], hashes)


def collect_source(root: Path) -> dict[str, bytes]:
    files = {root / name for name in BUILD_FILES}
    files.update(
        path for path in (root / "src").rglob("*.py") if "__pycache__" not in path.parts
    )
    files.update((root / "src/openapi").glob("*.json"))
    for folder in ("native", "container"):
        files.update(path for path in (root / "containers/cfb" / folder).glob("*") if path.is_file())
    vendor = root / "vendor/vnpy_ctp"
    for pattern in ("*.tar.gz", "*.patch", "README.md", "upstream.json", "SHA256SUMS"):
        files.update(vendor.glob(pattern))
    if not (root / "src/main.py").is_file() or not list(vendor.glob("*.tar.gz")):
        raise DeploymentError("构建源码缺少 src/main.py 或 CTP 源码包")
    result = {}
    for path in sorted(files):
        relative = path.relative_to(root)
        if (
            not allowed_path(relative.as_posix())
            or not path.is_file()
            or any(
                (root / Path(*relative.parts[:index])).is_symlink()
                for index in range(1, len(relative.parts) + 1)
            )
        ):
            raise DeploymentError(f"构建输入必须是白名单普通文件：{relative}")
        result[relative.as_posix()] = path.read_bytes()
    return result


def prepare_source(root: Path, workspace: Path, inventory: SourceInventory) -> dict:
    # 摘要与归档来自同一份字节快照，编辑源码不会在两次读取间破坏清单。
    files = collect_source(root)
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in files.items()}
    (workspace / "source.manifest").write_text(
        "".join(f"{digest}  {name}\n" for name, digest in hashes.items())
    )
    (workspace / "source.base").write_text(inventory.identity + "\n")
    changed = {
        name: data
        for name, data in files.items()
        if inventory.hashes.get(name) != hashes[name]
    }
    with (
        (workspace / "source.delta.tar.gz").open("wb") as stream,
        gzip.GzipFile(
            filename="", fileobj=stream, mode="wb", mtime=0, compresslevel=1
        ) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for name, data in changed.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(data))
    return {
        "changed": len(changed),
        "total": len(files),
        "bytes": sum(map(len, changed.values())),
    }
