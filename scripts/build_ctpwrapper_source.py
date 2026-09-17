"""从已校验的上游 sdist 重建固定的 CTP 补丁源码包，不修改当前 Python 环境。"""

import argparse
import gzip
import hashlib
import io
import subprocess
import tarfile
import tempfile
from pathlib import Path

VENDOR = Path(__file__).resolve().parents[1] / "vendor" / "ctpwrapper"
UPSTREAM_SHA256 = "94be58b8360e26f6c3b57f5a991a6c5bdac0f7707234e41ceb69c42c006e778e"
VERSION = "6.7.13+ccxtproxy.1"
ARCHIVE = f"ctpwrapper-{VERSION}.tar.gz"


def build_archive(source: Path) -> bytes:
    upstream = source.read_bytes()
    if hashlib.sha256(upstream).hexdigest() != UPSTREAM_SHA256:
        raise ValueError("Upstream ctpwrapper 6.7.13 source checksum mismatch")
    with tempfile.TemporaryDirectory(prefix="ctpwrapper-build-") as temporary:
        directory = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(upstream)) as archive:
            archive.extractall(directory, filter="data")
        root = directory / "ctpwrapper-6.7.13"
        subprocess.run(
            [
                "patch",
                "--batch",
                "--fuzz=0",
                "-p1",
                "-i",
                str(VENDOR / "release-gil.patch"),
            ],
            cwd=root,
            check=True,
            capture_output=True,
        )
        # 固定排序、时间、属主及 gzip header，重建应得到逐字节一致的产物。
        output = io.BytesIO()
        with gzip.GzipFile(
            fileobj=output, mode="wb", filename="", mtime=0
        ) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in sorted(root.rglob("*")):
                    if not path.is_file():
                        continue
                    member = tarfile.TarInfo(
                        f"ctpwrapper-{VERSION}/{path.relative_to(root).as_posix()}"
                    )
                    data = path.read_bytes()
                    member.size = len(data)
                    member.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                    archive.addfile(member, io.BytesIO(data))
        return output.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, required=True, help="官方 ctpwrapper-6.7.13.tar.gz"
    )
    parser.add_argument(
        "--check", action="store_true", help="只核对重建结果，不覆盖产物"
    )
    args = parser.parse_args()
    data = build_archive(args.source)
    checksum = f"{hashlib.sha256(data).hexdigest()}  {ARCHIVE}\n"
    target = VENDOR / ARCHIVE
    checksum_path = VENDOR / "SHA256SUMS"
    if args.check:
        if target.read_bytes() != data or checksum_path.read_text() != checksum:
            raise SystemExit("Patched source archive differs from reproducible build")
        print("Patched source archive and checksum verified")
    else:
        target.write_bytes(data)
        checksum_path.write_text(checksum)
        print(f"Built {target.name}: {hashlib.sha256(data).hexdigest()}")


if __name__ == "__main__":
    main()
