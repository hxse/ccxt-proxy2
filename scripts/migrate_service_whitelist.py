"""将现有 TOML 原地迁移到 service_whitelist，保留 0600 备份，不连接任何服务。"""

import argparse
import os
import sys
import tempfile
import tomllib
from pathlib import Path

from pydantic import ValidationError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.migrate_config_to_toml import render_toml  # noqa: E402
from src.tools.config_loader import (  # noqa: E402
    ConfigError,
    load_config,
    resolve_config_path,
)
from src.tools.config_migration import migrate_whitelist_payload  # noqa: E402
from src.tools.config_types import AppConfig  # noqa: E402


def migrate_service_whitelist(path: Path) -> Path:
    backup = path.with_name(path.name + ".before-service-whitelist.bak")
    if path.is_symlink() or backup.exists() or backup.is_symlink():
        raise ConfigError("Config is a symlink or backup exists; refusing to overwrite")
    try:
        original = path.read_bytes()
        payload = tomllib.loads(original.decode("utf-8"))
        if "service_whitelist" in payload:
            raise ConfigError(
                "service_whitelist already exists; no migration performed"
            )
        expected = AppConfig.model_validate(migrate_whitelist_payload(payload))
        content = render_toml(expected)
    except (OSError, ValueError, TypeError, AttributeError, ValidationError):
        raise ConfigError(
            "Cannot migrate configuration; values hidden; original retained"
        ) from None
    fd, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        if (
            load_config(temporary, environ={}) != expected
            or path.read_bytes() != original
        ):
            raise ConfigError(
                "Migration verification failed or source changed; original retained"
            )
        path.chmod(0o600)
        backup.hardlink_to(path)
        os.replace(temporary, path)
    except Exception:
        raise ConfigError(
            "Migration verification or replacement failed; values hidden; backup retained if created"
        ) from None
    finally:
        temporary.unlink(missing_ok=True)
    return backup


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=resolve_config_path())
    args = parser.parse_args()
    try:
        backup = migrate_service_whitelist(args.config)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"Migrated service_whitelist; verified all settings; backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
