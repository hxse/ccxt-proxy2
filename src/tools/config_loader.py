"""读取 TOML 并交给既有 AppConfig 校验，不初始化任何 Provider。"""

import os
import tomllib
from collections.abc import Mapping
from pathlib import Path

from pydantic import ValidationError

from src.tools.config_types import AppConfig

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH_VARIABLE = "CCXT_PROXY_CONFIG_PATH"


class ConfigError(RuntimeError):
    """只包含诊断信息，绝不携带配置值。"""


def resolve_config_path(environ: Mapping[str, str] | None = None) -> Path:
    env = os.environ if environ is None else environ
    if "CCXT_PROXY_ENV_FILE" in env and CONFIG_PATH_VARIABLE not in env:
        raise ConfigError(
            "Use CCXT_PROXY_CONFIG_PATH to select a TOML file; CCXT_PROXY_ENV_FILE is no longer supported"
        )
    path = Path(env.get(CONFIG_PATH_VARIABLE, "config.toml"))
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_config(
    path: Path | None = None, *, environ: Mapping[str, str] | None = None
) -> AppConfig:
    """只从 TOML 读取配置；环境变量仅用于选择配置文件，不覆盖账号字段。"""
    selected = resolve_config_path(environ) if path is None else Path(path)
    try:
        with selected.open("rb") as stream:
            payload = tomllib.load(stream)
    except FileNotFoundError:
        raise ConfigError(
            "Config file not found; copy config.example.toml to config.toml or set CCXT_PROXY_CONFIG_PATH"
        ) from None
    except (tomllib.TOMLDecodeError, UnicodeError):
        # TOMLDecodeError 的原始消息可能包含用户自定义键，不能直接输出。
        raise ConfigError(
            "Invalid TOML syntax; check config.example.toml; values hidden"
        ) from None
    except OSError:
        raise ConfigError(
            "Cannot read TOML config; check file path and permissions"
        ) from None
    try:
        if "exchange_whitelist" in payload:
            raise ConfigError(
                "exchange_whitelist was replaced by service_whitelist; run scripts/migrate_service_whitelist.py"
            )
        return AppConfig.model_validate(payload)
    except ValidationError as exc:
        # 不输出原始值、用户名、validator 上下文或异常链。
        fields = sorted(
            {
                str(error["loc"][0])
                for error in exc.errors()
                if error["loc"] and error["loc"][0] in AppConfig.model_fields
            }
        )
        scope = ", ".join(fields) or "settings"
        raise ConfigError(
            f"Invalid TOML configuration ({scope}); check required fields and config.example.toml; values hidden"
        ) from None
