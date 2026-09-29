"""构建时获取固定终端安装包，校验后才交给解包阶段。"""

import hashlib
from pathlib import Path
import sys
import tomllib
import urllib.request


def fetch(lock: Path, target: Path) -> None:
    values = tomllib.loads(lock.read_text())
    if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == values["sha256"]:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".partial")
    try:
        with urllib.request.urlopen(values["url"], timeout=30) as response, partial.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
        if hashlib.sha256(partial.read_bytes()).hexdigest() != values["sha256"]:
            raise ValueError("terminal installer SHA256 mismatch")
        partial.replace(target)
    finally:
        partial.unlink(missing_ok=True)


if __name__ == "__main__":
    fetch(Path(sys.argv[1]), Path(sys.argv[2]))
