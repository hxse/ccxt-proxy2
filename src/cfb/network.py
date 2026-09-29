"""CFB 的 TCP 出口：代理仅绑定执行器子进程，不修改主服务环境。"""

import os
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

from .config import Settings

PROXY_LIBRARIES = (
    Path("/usr/lib/i386-linux-gnu/libproxychains.so.4"),
    Path("/usr/lib/x86_64-linux-gnu/libproxychains.so.4"),
)


def parse_proxy(value: str | None) -> tuple[str, int, str, str]:
    try:
        if not value:
            raise ValueError
        url = urlsplit(value)
        if url.scheme != "http" or not url.hostname or url.path not in ("", "/") or url.query or url.fragment:
            raise ValueError
        host, port = url.hostname, url.port or 80
        user, password = unquote(url.username or ""), unquote(url.password or "")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", host) or not 1 <= port <= 65535:
            raise ValueError
        if bool(user) != bool(password) or any(re.search(r"[^!-~]", part) for part in (user, password)):
            raise ValueError
        return host, port, user, password
    except (ValueError, TypeError):
        raise ValueError("cfb proxy requires a valid http CONNECT address; values hidden") from None


def prepare_proxy(settings: Settings, value: str) -> None:
    host, port, user, password = parse_proxy(value)
    for path, elf_class in zip(PROXY_LIBRARIES, (1, 2), strict=True):
        with path.open("rb") as stream:
            header = stream.read(5)
        if header != b"\x7fELF" + bytes([elf_class]):
            raise ValueError("CFB proxy library missing or architecture mismatch")
    directory = settings.bridge.data_dir / "run"
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = directory / "proxychains.conf"
    configuration = (
        "strict_chain\nproxy_dns\nquiet_mode\n"
        "tcp_read_time_out 10000\ntcp_connect_time_out 5000\n"
        "localnet 127.0.0.0/255.0.0.0\nlocalnet ::1/128\n"
        "[ProxyList]\n"
        + f"http {host} {port}" + (f" {user} {password}" if user else "") + "\n"
    )
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(configuration)
    target.chmod(0o600)
    settings._proxy_config = str(target)


def process_environment(settings: Settings, base: dict[str, str]) -> dict[str, str]:
    result = {
        key: value for key, value in base.items()
        if key.lower() not in {"http_proxy", "https_proxy", "all_proxy", "no_proxy"}
        and key not in {"LD_PRELOAD", "PROXYCHAINS_CONF_FILE", "PROXYCHAINS_QUIET_MODE"}
    }
    if settings._proxy_config is not None:
        # 使用库名，让 ELF 动态加载器为 32/64 位进程选择对应 multiarch 库。
        result.update(LD_PRELOAD="libproxychains.so.4",
                      PROXYCHAINS_CONF_FILE=settings._proxy_config,
                      PROXYCHAINS_QUIET_MODE="1")
    return result
