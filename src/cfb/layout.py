"""部署只输出已验证布局和配置摘要，不输出账号或代理地址。"""

import argparse
import hashlib
import json
from pathlib import Path

from src.tools.config_loader import load_config


def layout(config) -> str:
    settings = config.cfb
    if settings is None or not any(item.service == "cfb" for item in config.service_whitelist):
        return "false none false 45174 45175"
    identity = settings.model_dump(mode="json", exclude={"accounts"})
    identity["selected_account"] = {
        "broker_id": settings.broker_id, "site": settings.site,
        "username": settings.account.username.get_secret_value(),
        "password": settings.account.password.get_secret_value(),
    }
    identity["proxy"] = config.proxy.effective_http if settings.enable_proxy else None
    digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return f"true {digest} {str(settings.vnc_enabled).lower()} {settings.vnc.port} {settings.vnc.web_port}"


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
