import stat
import tomllib

import pytest
from pydantic import ValidationError

from scripts.migrate_service_whitelist import migrate_service_whitelist
from src.tools.config_loader import ConfigError, load_config
from src.tools.config_types import AppConfig
from Test.ctp_fakes import ctp_config


def settings(tmp_path):
    return {
        "SECRET": "offline",
        "tq": {"username": "test", "password": "test"},
        "binance": {"test": {"api_key": "test", "secret": "test"}},
        "ctp": ctp_config(tmp_path).model_dump() | {"query_interval_seconds": 1.1},
    }


def test_whitelist_types_have_distinct_strict_fields(tmp_path):
    payload = settings(tmp_path)
    entries = [
        {
            "service": "ccxt",
            "exchange": "binance",
            "market": "future",
            "mode": "sandbox",
        },
        {"service": "tq"},
        {"service": "ctp", "mode": "sandbox"},
    ]
    config = AppConfig.model_validate(payload | {"service_whitelist": entries})
    assert [item.identity for item in config.service_whitelist] == [
        "ccxt/binance/future/sandbox",
        "tq",
        "ctp/sandbox",
    ]
    assert AppConfig.model_validate(payload).service_whitelist == []


@pytest.mark.parametrize(
    "entry",
    [
        {"service": "tq", "mode": "live"},
        {"service": "tq", "exchange": "binance"},
        {"service": "ctp"},
        {"service": "ctp", "mode": "test"},
        {"service": "ctp", "mode": "sandbox", "market": "future"},
        {"service": "unknown"},
        {"service": "ccxt", "exchange": "binance"},
    ],
)
def test_invalid_service_fields_are_rejected(tmp_path, entry):
    with pytest.raises(ValidationError):
        AppConfig.model_validate(settings(tmp_path) | {"service_whitelist": [entry]})


@pytest.mark.parametrize(
    "entry", [{"service": "tq"}, {"service": "ctp", "mode": "live"}]
)
def test_missing_or_duplicate_service_configuration_is_rejected(tmp_path, entry):
    with pytest.raises(ValidationError, match="missing"):
        AppConfig.model_validate({"SECRET": "test", "service_whitelist": [entry]})
    with pytest.raises(ValidationError, match="duplicate service_whitelist"):
        AppConfig.model_validate(
            settings(tmp_path) | {"service_whitelist": [entry, entry]}
        )


def test_existing_toml_migration_preserves_values_and_private_backup(tmp_path):
    path = tmp_path / "config.toml"
    original = """SECRET = 'offline'
[users.'test.user']
password = '${UNCHANGED} # 密码'
[tq]
username = 'test'
password = 'test'
[binance.test]
api_key = 'key'
secret = 'secret'
[[exchange_whitelist]]
exchange = 'binance'
market = 'future'
mode = 'sandbox'
"""
    path.write_text(original)
    with pytest.raises(ConfigError, match="migrate_service_whitelist"):
        load_config(path)
    backup = migrate_service_whitelist(path)
    assert backup.read_text() == original
    before = tomllib.loads(original)
    after = tomllib.loads(path.read_text())
    before.pop("exchange_whitelist")
    after.pop("service_whitelist")

    def assert_preserved(old, new):
        if isinstance(old, dict):
            for key, value in old.items():
                assert_preserved(value, new[key])
        else:
            assert new == old

    assert_preserved(before, after)
    assert [v.identity for v in load_config(path).service_whitelist] == [
        "ccxt/binance/future/sandbox",
        "tq",
    ]
    assert (
        stat.S_IMODE(path.stat().st_mode)
        == stat.S_IMODE(backup.stat().st_mode)
        == 0o600
    )
    with pytest.raises(ConfigError):
        migrate_service_whitelist(path)
    assert backup.read_text() == original


def test_migration_rejects_mixed_formats_and_invalid_values_without_exposing_them(
    tmp_path,
):
    path = tmp_path / "config.toml"
    path.write_text(
        "SECRET = 'do-not-expose'\nexchange_whitelist = []\nservice_whitelist = []\n"
    )
    before = path.read_bytes()
    with pytest.raises(ConfigError) as caught:
        migrate_service_whitelist(path)
    assert "do-not-expose" not in str(caught.value)
    assert path.read_bytes() == before
