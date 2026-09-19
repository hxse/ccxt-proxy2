"""校验 VeighNa 官方源码，重建交易 API 补丁包；可跟随官方 PyPI 发布升级。"""

import argparse
import ast
import gzip
import hashlib
import io
import json
import os
import re
import ssl
import subprocess
import tarfile
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "vendor" / "vnpy_ctp"
PATCH_SUFFIX = "+ccxtproxy.2"
CPP = "vnpy_ctp/api/vnctp/vnctptd/vnctptd.cpp"


def download(url: str, hostname: str) -> bytes:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != hostname:
        raise ValueError("Unexpected upstream download origin")
    context = ssl.create_default_context()
    # NixOS 的 CA bundle 不一定出现在 Python/OpenSSL 默认的证书文件路径。
    # 显式 SSL_CERT_FILE/SSL_CERT_DIR 保持优先；全程验证证书与主机名。
    if not os.environ.get("SSL_CERT_FILE") and not os.environ.get("SSL_CERT_DIR"):
        bundle = Path(
            os.environ.get("NIX_SSL_CERT_FILE", "/etc/ssl/certs/ca-certificates.crt")
        )
        if bundle.is_file():
            context.load_verify_locations(cafile=str(bundle))
    with urlopen(url, timeout=60, context=context) as response:
        if urlparse(response.url).hostname != hostname:
            raise ValueError("Unexpected upstream redirect")
        return response.read()


def published_release(version: str) -> dict[str, str]:
    if version != "latest" and not re.fullmatch(r"\d+(?:\.\d+){1,3}", version):
        raise ValueError("Expected a stable numeric VeighNa release version")
    suffix = "" if version == "latest" else f"/{version}"
    release = json.loads(
        download(f"https://pypi.org/pypi/vnpy_ctp{suffix}/json", "pypi.org")
    )
    version = release["info"]["version"]
    if not re.fullmatch(r"\d+(?:\.\d+){1,3}", version):
        raise ValueError("Only stable VeighNa releases are supported")
    sources = [
        item
        for item in release["urls"]
        if item["packagetype"] == "sdist" and not item.get("yanked")
    ]
    if len(sources) != 1:
        raise ValueError("Expected one non-yanked official source distribution")
    source = sources[0]
    return {
        "version": version,
        "url": source["url"],
        "sha256": source["digests"]["sha256"],
    }


def check_response_fields(root: Path) -> None:
    """新版新增或移除回报字段时停止升级，避免详细 HTTP 模型静默过期。"""
    source = (root / CPP).read_text()
    groups = {
        "src/ctp_records_trading.py": {
            "CtpOrder": "processRspQryOrder",
            "CtpTrade": "processRspQryTrade",
        },
        "src/ctp_records_account.py": {
            "CtpPosition": "processRspQryInvestorPosition",
            "CtpTradingAccount": "processRspQryTradingAccount",
        },
        "src/responses_ctp.py": {"CtpInstrumentStatus": "processRtnInstrumentStatus"},
    }
    for file, models in groups.items():
        classes = {
            node.name: node
            for node in ast.parse((ROOT / file).read_text()).body
            if isinstance(node, ast.ClassDef)
        }
        for name, method in models.items():
            expected = {
                node.target.id
                for node in classes[name].body
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
            }
            match = re.search(
                r"void TdApi::" + method + r"\(.*?(?=\nvoid TdApi::)", source, re.S
            )
            if match is None:
                raise ValueError(f"Upstream callback not found: {method}")
            actual = {
                key
                for key in re.findall(r'data\["([^"]+)"\]', match.group())
                if not key.startswith("reserve")
            }
            if actual != expected:
                raise ValueError(
                    f"Review {name} before upgrading: added={sorted(actual - expected)}, "
                    f"removed={sorted(expected - actual)}"
                )


def own_callback_payloads(source: str) -> str:
    """给生成的回调代码统一加上所有权；任何未匹配的结构变化都停止构建。"""
    for kind in ("data", "error"):
        member = "task_" + kind
        source, allocations = re.subn(
            rf"\b(CThostFtdc\w+)\s*\*\s*{member}\s*=\s*new\s+\1\(\);",
            rf"auto {member} = make_shared<\1>();",
            source,
        )
        source, casts = re.subn(
            rf"(\(CThostFtdc\w+\s*\*\)\s*task->{member})(?=\s*;)",
            r"\1.get()",
            source,
        )
        source, deletions = re.subn(
            rf"^[ \t]*delete {member};\n", "", source, flags=re.M
        )
        assignments = len(re.findall(rf"task\.{member}\s*=\s*{member};", source))
        if not (allocations == casts == deletions == assignments > 0):
            raise ValueError(
                f"Review upstream callback ownership for {member}: "
                f"allocations={allocations}, casts={casts}, "
                f"deletions={deletions}, assignments={assignments}"
            )
    return source


def build_archive(upstream: bytes, manifest: dict[str, str]) -> bytes:
    if hashlib.sha256(upstream).hexdigest() != manifest["sha256"]:
        raise ValueError("VeighNa source checksum mismatch")
    version = manifest["version"] + PATCH_SUFFIX
    with tempfile.TemporaryDirectory(prefix="vnpy-ctp-build-") as temporary:
        directory = Path(temporary)
        with tarfile.open(fileobj=io.BytesIO(upstream)) as archive:
            archive.extractall(directory, filter="data")
        root = directory / f"vnpy_ctp-{manifest['version']}"
        check_response_fields(root)
        # 上游同时使用 CRLF/LF；只规范补丁涉及的文本文件，原生二进制不变。
        patch = (VENDOR / "trading-api.patch").read_text()
        for name in re.findall(r"^--- a/(.+)$", patch, re.M):
            path = root / name
            path.write_bytes(path.read_bytes().replace(b"\r\n", b"\n"))
        try:
            subprocess.run(
                [
                    "patch",
                    "--batch",
                    "--fuzz=0",
                    "-p1",
                    "-i",
                    str(VENDOR / "trading-api.patch"),
                ],
                cwd=root,
                check=True,
                capture_output=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ValueError(
                "Upstream changed; review trading-api.patch before upgrading"
            ) from exc
        # 这些函数由上游生成；逐项核对同一批分配、读取、释放和入队赋值，
        # 使 Task 持有数据直到处理结束或被队列丢弃，避免复制/异常路径重复释放。
        callback_source = root / CPP
        callback_source.write_text(own_callback_payloads(callback_source.read_text()))
        for file in ("pyproject.toml", "meson.build"):
            path = root / file
            text = path.read_text()
            old = 'version = "' if file == "pyproject.toml" else "version: '"
            old += manifest["version"]
            if text.count(old) != 1:
                raise ValueError(f"Unexpected upstream version metadata: {file}")
            path.write_text(text.replace(old, old + PATCH_SUFFIX, 1))
        for path in root.rglob("PKG-INFO"):
            text = path.read_text()
            path.write_text(
                text.replace(
                    f"Version: {manifest['version']}\n", f"Version: {version}\n"
                )
            )
        output = io.BytesIO()
        with gzip.GzipFile(
            fileobj=output, mode="wb", filename="", mtime=0
        ) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in sorted(root.rglob("*")):
                    if not path.is_file():
                        continue
                    data = path.read_bytes()
                    member = tarfile.TarInfo(
                        f"vnpy_ctp-{version}/{path.relative_to(root).as_posix()}"
                    )
                    member.size = len(data)
                    member.mode = 0o755 if path.stat().st_mode & 0o111 else 0o644
                    archive.addfile(member, io.BytesIO(data))
        return output.getvalue()


def install_archive(data: bytes, manifest: dict[str, str], check: bool) -> None:
    version = manifest["version"] + PATCH_SUFFIX
    name = f"vnpy_ctp-{version}.tar.gz"
    target = VENDOR / name
    checksum = f"{hashlib.sha256(data).hexdigest()}  {name}\n"
    if check:
        if (
            target.read_bytes() != data
            or (VENDOR / "SHA256SUMS").read_text() != checksum
        ):
            raise ValueError("Vendored source differs from reproducible build")
        print(f"Verified {name}")
        return
    project = ROOT / "pyproject.toml"
    text, count = re.subn(
        r'^ctp = \["vnpy_ctp==[^"\n]+"\]$',
        f'ctp = ["vnpy_ctp=={version}"]',
        project.read_text(),
        flags=re.M,
    )
    text, sources = re.subn(
        r'^vnpy_ctp = \{ path = "vendor/vnpy_ctp/[^"\n]+" \}$',
        f'vnpy_ctp = {{ path = "vendor/vnpy_ctp/{name}" }}',
        text,
        flags=re.M,
    )
    if count != 1 or sources != 1:
        raise ValueError(
            "Expected one CTP extra and one local source in pyproject.toml"
        )
    contents = {
        target: data,
        VENDOR / "SHA256SUMS": checksum.encode(),
        VENDOR / "upstream.json": (json.dumps(manifest, indent=2) + "\n").encode(),
        project: text.encode(),
    }
    for path, value in contents.items():
        # 重复检查同一版本时保留文件时间，避免 uv 无谓地重编译原生扩展。
        if not path.exists() or path.read_bytes() != value:
            path.write_bytes(value)
    for previous in VENDOR.glob("vnpy_ctp-*.tar.gz"):
        if previous != target:
            previous.unlink()
    print(f"Built {name}: {hashlib.sha256(data).hexdigest()}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="本地官方 sdist；省略时按清单下载")
    parser.add_argument(
        "--check", action="store_true", help="只核对可重复构建，不修改项目"
    )
    parser.add_argument(
        "--upgrade", nargs="?", const="latest", help="获取官方最新稳定版或指定版本"
    )
    args = parser.parse_args()
    if args.check and args.upgrade:
        parser.error("--check and --upgrade cannot be combined")
    manifest = (
        published_release(args.upgrade)
        if args.upgrade
        else json.loads((VENDOR / "upstream.json").read_text())
    )
    upstream = (
        args.source.read_bytes()
        if args.source
        else download(manifest["url"], "files.pythonhosted.org")
    )
    install_archive(build_archive(upstream, manifest), manifest, args.check)


if __name__ == "__main__":
    main()
