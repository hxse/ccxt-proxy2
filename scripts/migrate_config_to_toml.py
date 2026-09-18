"""一次性把旧 .env 或 JSON 迁到 TOML；不初始化应用或交易连接。"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import tomli_w
from dotenv.parser import parse_stream
from pydantic import SecretStr

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.tools.config_loader import ConfigError, load_config  # noqa: E402
from src.tools.config_migration import migrate_whitelist_payload  # noqa: E402
from src.tools.config_types import AppConfig  # noqa: E402

ENV_PREFIX = "CCXT_PROXY_"
JSON_PATHS = {("users",), ("exchange_whitelist",), ("telegram", "chats")}


def _read_legacy_env(source: Path) -> dict[str, Any]:
    """仅解析旧文件，不继承进程环境、不展开密码中的变量。"""
    payload: dict[str, Any] = {}
    with source.open(encoding="utf-8") as stream:
        for binding in parse_stream(stream):
            if binding.error:
                raise ConfigError("Invalid legacy .env syntax; values hidden")
            if binding.key is None or not binding.key.startswith(ENV_PREFIX):
                continue
            if binding.key == "CCXT_PROXY_ENV_FILE":
                continue
            if binding.value is None:
                raise ConfigError("Missing legacy .env assignment; values hidden")
            parts = binding.key.removeprefix(ENV_PREFIX).lower().split("__")
            if not all(parts):
                raise ConfigError("Invalid legacy nested setting name; values hidden")
            if parts[0] == "secret":
                parts[0] = "SECRET"
            value: Any = binding.value
            if tuple(parts) in JSON_PATHS:
                value = json.loads(value)
            node = payload
            for part in parts[:-1]:
                node = node.setdefault(part, {})
                if not isinstance(node, dict):
                    raise ConfigError("Conflicting legacy .env settings; values hidden")
            if parts[-1] in node:
                raise ConfigError("Duplicate legacy .env setting; values hidden")
            node[parts[-1]] = value
    return payload


def read_legacy_config(source: Path) -> AppConfig:
    try:
        if source.name.endswith((".json", ".json.bak")):
            payload = json.loads(source.read_text(encoding="utf-8"))
        else:
            payload = _read_legacy_env(source)
        return AppConfig.model_validate(migrate_whitelist_payload(payload))
    except (OSError, UnicodeError):
        raise ConfigError(
            "Cannot read legacy config; check path and permissions"
        ) from None
    except (ValueError, TypeError, AttributeError):
        raise ConfigError("Invalid legacy configuration; values hidden") from None


def _plain(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    return value


def render_toml(config: AppConfig) -> str:
    # TOML 无 null；缺省字段仍由原有 AppConfig 提供相同默认值。
    payload = _plain(config.model_dump(mode="python", exclude_none=True, by_alias=True))
    whitelist = payload.pop("service_whitelist")
    content = (
        "# 本地实际配置，请勿提交；字段说明见 config.example.toml。\n"
        "# 修改后重启服务。未配置的可选字段/分组直接省略。\n\n" + tomli_w.dumps(payload)
    )
    # 显式使用数组表，避免把白名单挤成内联对象；值的转义仍交给 TOML writer。
    for item in whitelist:
        content += "\n[[service_whitelist]]\n" + tomli_w.dumps(item)
    return content


def migrate_config(source: Path, target: Path) -> Path:
    backup = source.with_name(source.name + ".bak")
    if target.exists() or target.is_symlink():
        raise ConfigError("Target config already exists; refusing to overwrite")
    if backup.exists() or backup.is_symlink():
        raise ConfigError("Legacy backup already exists; refusing to overwrite")
    expected = read_legacy_config(source)
    content = render_toml(expected)
    try:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError:
        raise ConfigError(
            "Cannot create TOML config; existing files were not changed"
        ) from None
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
        if load_config(target, environ={}) != expected:
            raise ConfigError("TOML migration verification failed; original retained")
        source.chmod(0o600)
        # 同目录创建硬链接，原子拒绝覆盖已有备份，再移除旧入口。
        backup.hardlink_to(source)
        source.unlink()
    except Exception:
        target.unlink()
        raise ConfigError(
            "TOML migration verification or backup failed; values hidden"
        ) from None
    return backup


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--target", type=Path, default=PROJECT_ROOT / "config.toml")
    args = parser.parse_args()
    try:
        backup = migrate_config(args.source, args.target)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        f"Migrated to {args.target}; verified all settings; original saved as {backup}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
