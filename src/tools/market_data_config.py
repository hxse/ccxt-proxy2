"""公开计划固定在项目根目录；私有配置沿用原加载器。"""

import tomllib
from pathlib import Path

from pydantic import ValidationError

from src.tools.config_loader import ConfigError
from src.tools.config_types import AppConfig
from src.tools.market_data_types import MarketDataPlan

PLAN_PATH = Path(__file__).resolve().parents[2] / "market_data.toml"


def load_market_data_plan(path: Path | None = None) -> MarketDataPlan:
    try:
        with (path or PLAN_PATH).open("rb") as stream:
            return MarketDataPlan.model_validate(tomllib.load(stream))
    except (OSError, ValueError, ValidationError):
        raise ConfigError(
            "Invalid or missing market_data.toml; check the documented plan; values hidden"
        ) from None


def tq_enabled(config: AppConfig) -> bool:
    return any(item.service == "tq" for item in config.service_whitelist)


def has_work(plan: MarketDataPlan, config: AppConfig, scope: str = "both") -> bool:
    return (
        scope in {"both", "collect"}
        and plan.tq_collection.has_work
        and tq_enabled(config)
    ) or (scope in {"both", "prune"} and plan.retention.enabled)


def validate_client(
    plan: MarketDataPlan, config: AppConfig, scope: str = "both"
) -> None:
    if has_work(plan, config, scope) and (
        config.market_data_client.user is None
        or config.market_data_client.user not in config.users
    ):
        raise ConfigError(
            "market_data_client.user must reference an existing users entry for enabled background HTTP work; values hidden"
        )
