import tomllib
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.tools.config_loader import ConfigError, load_config
from src.tools.config_types import AppConfig
from src.tools.market_data_config import (
    has_work,
    load_market_data_plan,
    validate_client,
)
from src.tools.market_data_types import MarketDataClientConfig, MarketDataPlan

ROOT = Path(__file__).resolve().parents[1]


def payload():
    return tomllib.loads((ROOT / "market_data.toml").read_text())


def job_config(tq=True):
    return AppConfig.model_validate(
        {
            "SECRET": "offline-background-secret-32-bytes",
            "users": {"worker": {"password": "do-not-log-password"}},
            "market_data_client": {"user": "worker"},
            "tq": {"username": "offline", "password": "offline"},
            "service_whitelist": [{"service": "tq"}] if tq else [],
        }
    )


def test_frozen_examples_load_and_every_parameter_is_commented(monkeypatch, tmp_path):
    spec = (
        ROOT
        / "doc/task_specs/20260926A.007 market data background jobs/spec_01_configuration.md"
    ).read_text()
    example = spec.split("```toml\n")[1].split("```")[0]
    assert (ROOT / "market_data.toml").read_text().strip() == example.strip()
    for index, line in enumerate(example.splitlines()):
        if "=" in line and not line.startswith("#"):
            assert "#" in line or example.splitlines()[index - 1].startswith("#")
    monkeypatch.chdir(tmp_path)
    plan = load_market_data_plan(ROOT / "market_data.toml")
    assert len(plan.tq_collection.symbols) == 28
    assert (
        plan.tq_collection.mapping_timeframes
        == plan.tq_collection.transition_timeframes
        == ["5m"]
    )
    config = load_config(ROOT / "config.example.toml")
    validate_client(plan, config)
    assert config.market_data_client.user == "admin"
    assert plan.retention.modes.sandbox == 0


def test_defaults_replacements_and_disabled_plan():
    plan = MarketDataPlan.model_validate(
        {"tq_collection": {"symbols": {"ma": "KQ.m@CZCE.MA"}}}
    )
    assert (
        plan.pipeline.interval_seconds == 3600 and len(plan.tq_collection.symbols) == 1
    )
    plan = MarketDataPlan.model_validate(
        {"tq_collection": {"enabled": False}, "retention": {"enabled": False}}
    )
    config = AppConfig.model_validate({"SECRET": "offline"})
    validate_client(plan, config)
    assert not has_work(plan, config)
    # 没有 TQ 也仍可单独清理，因此需要 HTTP 账号。
    plan.retention.enabled = True
    with pytest.raises(ConfigError, match="user"):
        validate_client(plan, config)
    validate_client(plan, job_config(tq=False))


@pytest.mark.parametrize(
    "group,field,value",
    [
        ("pipeline", "interval_seconds", True),
        ("pipeline", "interval_seconds", 0),
        ("pipeline", "typo", 1),
        ("tq_collection", "enabled", "true"),
        ("tq_collection", "data_length", 10001),
        ("tq_collection", "timeframes", ["1M"]),
        ("tq_collection", "timeframes", ["5m", "5m"]),
        ("tq_collection", "save_main", False),
        ("tq_collection", "mapping_timeframes", []),
        ("tq_collection", "transition_timeframes", ["1h"]),
        ("tq_collection", "symbols", {"a": "KQ.m@SHFE.rb", "b": "KQ.m@SHFE.rb"}),
        ("tq_collection", "symbols", {"a": "KQ.i@SHFE.rb"}),
        ("retention", "providers", ["ccxt", "ccxt"]),
        ("retention", "modes", {"live": -1}),
        ("retention", "modes", {"sandbox": False}),
    ],
)
def test_invalid_plans_fail_before_requests(group, field, value):
    data = payload()
    data[group][field] = value
    with pytest.raises(ValidationError):
        MarketDataPlan.model_validate(data)


@pytest.mark.parametrize(
    "value",
    [
        "relative",
        "http://a@localhost",
        "http://localhost?token=secret",
        "http://localhost:99999",
        "ftp://localhost",
    ],
)
def test_invalid_client_urls(value):
    with pytest.raises(ValidationError):
        MarketDataClientConfig(base_url=value)


def test_missing_or_invalid_plan_is_not_silently_disabled(tmp_path):
    path = tmp_path / "missing.toml"
    with pytest.raises(ConfigError, match="market_data.toml"):
        load_market_data_plan(path)
    path.write_text("secret-do-not-echo")
    with pytest.raises(ConfigError) as error:
        load_market_data_plan(path)
    assert "secret-do-not-echo" not in str(error.value)
