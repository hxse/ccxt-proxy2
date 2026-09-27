"""只打包镜像构建所需源码；私有运行配置始终独立传输。"""

import gzip
import tarfile
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
)


def pack_source(root: Path, target: Path):
    files = {root / name for name in BUILD_FILES}
    files.update(
        path for path in (root / "src").rglob("*.py") if "__pycache__" not in path.parts
    )
    files.update((root / "src/openapi").glob("*.json"))
    vendor = root / "vendor/vnpy_ctp"
    for pattern in ("*.tar.gz", "*.patch", "README.md", "upstream.json", "SHA256SUMS"):
        files.update(vendor.glob(pattern))
    if not (root / "src/main.py").is_file() or not list(vendor.glob("*.tar.gz")):
        raise DeploymentError("构建源码缺少 src/main.py 或 CTP 源码包")
    with (
        target.open("wb") as stream,
        gzip.GzipFile(
            filename="", fileobj=stream, mode="wb", mtime=0, compresslevel=1
        ) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for path in sorted(files):
            relative = path.relative_to(root)
            if not path.is_file() or any(
                (root / Path(*relative.parts[:index])).is_symlink()
                for index in range(1, len(relative.parts) + 1)
            ):
                raise DeploymentError(f"构建输入必须是普通文件：{relative}")
            info = archive.gettarinfo(str(path), arcname=relative.as_posix())
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with path.open("rb") as source:
                archive.addfile(info, source)
