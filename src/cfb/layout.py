"""部署只输出已验证模式、布局和配置摘要，不输出账号或代理地址。"""

import argparse
import hashlib
import json
from pathlib import Path

from src.base_types import ModeType
from src.tools.config_loader import load_config

from .config import Settings

MODES: tuple[ModeType, ...] = ("sandbox", "live")


def identity(settings: Settings, proxy: str | None) -> str:
    value = settings.model_dump(mode="json", exclude={"accounts"})
    value["selected_account"] = {
        "broker_id": settings.broker_id, "site": settings.site,
        "username": settings.account.username.get_secret_value(),
        "password": settings.account.password.get_secret_value(),
    }
    value["proxy"] = proxy if settings.enable_proxy else None
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def layout(config) -> str:
    modes = {item.mode for item in config.service_whitelist if item.service == "cfb"}
    if not modes:
        return "none"
    rows = []
    for mode in MODES:
        if mode not in modes:
            continue
        settings = config.cfb.for_mode(mode)
        digest = identity(settings, config.proxy.effective_http)
        rows.append(f"{mode} {digest} {str(settings.vnc_enabled).lower()} {settings.vnc.port} {settings.vnc.web_port}")
    return "\n".join(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="CFB 受管容器布局")
    parser.add_argument("--config", type=Path, default=Path("/app/config.toml"))
    args = parser.parse_args()
    try:
        print(layout(load_config(args.config)))
    except Exception as exc:
        print("CFB 配置无效：" + type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
