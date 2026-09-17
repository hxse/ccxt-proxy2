import json
import stat
import subprocess
from pathlib import Path

import pytest

from scripts.migrate_config_to_toml import (
    migrate_config,
    read_legacy_config,
    render_toml,
)
from src.tools.config_loader import ConfigError, load_config, resolve_config_path
from src.tools.config_types import AppConfig


def write_toml(tmp_path, text):
    path = tmp_path / "settings.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_toml_tables_and_arrays_preserve_existing_model(tmp_path):
    path = write_toml(
        tmp_path,
        """
SECRET = 'from-file'
[users.'Mixed.Case-user']
password = 'from-file'
[tq]
username = 'user'
password = '${DO_NOT_EXPAND} # literal'
[binance]
enable_proxy = false
[binance.test]
api_key = 'key'
secret = 'secret'
[telegram]
bot_token = 'test-token'
[telegram.chats]
MixedCase = '-123'
[[exchange_whitelist]]
exchange = 'binance'
market = 'future'
mode = 'sandbox'
""",
    )
    # 环境变量仅选择配置文件，不能覆盖文件里的账号、密码或签名密钥。
    config = load_config(path, environ={"CCXT_PROXY_SECRET": "process-secret"})
    assert config.SECRET == "from-file"
    assert config.users["Mixed.Case-user"].password == "from-file"
    assert config.tq is not None and config.binance is not None
    assert config.tq.password == "${DO_NOT_EXPAND} # literal"
    assert config.binance.enable_proxy is False
    assert config.telegram is not None and config.telegram.chats == {
        "MixedCase": "-123"
    }
    assert config.exchange_whitelist[0].mode == "sandbox"
    assert config.ctp is None


def test_default_file_and_missing_file_without_legacy_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr("src.tools.config_loader.PROJECT_ROOT", tmp_path)
    default = tmp_path / "config.toml"
    default.write_text("SECRET = 'toml-secret'")
    assert load_config(environ={}).SECRET == "toml-secret"
    default.unlink()
    (tmp_path / ".env").write_text("CCXT_PROXY_SECRET='legacy-secret'")
    with pytest.raises(ConfigError, match="Config file not found"):
        load_config(environ={"CCXT_PROXY_SECRET": "process-secret"})


def test_config_path_selector_uses_project_root_and_supports_absolute_paths(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    path = resolve_config_path({"CCXT_PROXY_CONFIG_PATH": "Test/fixtures/config.toml"})
    assert path.is_file()
    assert load_config(path, environ={}).exchange_whitelist == []
    selected = write_toml(tmp_path, "SECRET = 'selected'")
    assert (
        load_config(environ={"CCXT_PROXY_CONFIG_PATH": str(selected)}).SECRET
        == "selected"
    )


def test_legacy_file_selector_has_a_clear_migration_error():
    with pytest.raises(ConfigError, match="CCXT_PROXY_CONFIG_PATH"):
        resolve_config_path({"CCXT_PROXY_ENV_FILE": "old.env"})


@pytest.mark.parametrize(
    "text",
    [
        "SECRET = 'do-not-disclose\n",
        "SECRET = 'first'\nSECRET = 'do-not-disclose'\n",
        "SECRET = 'valid'\nusers = 'do-not-disclose'\n",
        "SECRET = 'valid'\n[tq]\npasword = 'do-not-disclose'\n",
        "SECRET = 'valid'\n[ctp.test]\npassword = 'do-not-disclose'\n",
        "SECRET = 'valid'\n[users.'do-not-disclose']\npassword = []\n",
        "SECRET = 'valid'\n[users.'do-not-disclose']\n[users.'do-not-disclose']\n",
    ],
)
def test_config_errors_do_not_reveal_values_or_usernames(tmp_path, text, caplog):
    with pytest.raises(ConfigError) as caught:
        load_config(write_toml(tmp_path, text), environ={})
    assert "do-not-disclose" not in str(caught.value)
    assert "do-not-disclose" not in caplog.text


def test_json_migration_roundtrips_credentials_and_keeps_private_backup(tmp_path):
    unusual = " 'quoted' \\ backslash\nnext\r\n$HOME ${SECRET} # 密码 "
    raw = {
        "SECRET": unusual,
        "users": {
            "Mixed.Case-user": {"password": unusual},
            "Another": {"password": "another-password"},
        },
        "proxy": {"_http": "http://127.0.0.1:7890"},
        "binance": {
            "enable_proxy": True,
            "test": {"api_key": unusual, "secret": unusual},
        },
        "kraken": {"live": {"api_key": "live-key", "secret": unusual}},
        "tq": {"username": "MixedCase", "password": unusual},
        "telegram": {"bot_token": "test-token", "chats": {"my-chat": "-123"}},
        "ctp": {
            "test": {
                "trader_front": "tcp://localhost:12345",
                "broker_id": "9999",
                "investor_id": "123456",
                "password": "abc'\\${KEEP} #",
                "app_id": "app",
                "auth_code": "test-code",
            },
            "live": {
                "trader_front": "tcp://localhost:12346",
                "broker_id": "0001",
                "investor_id": "654321",
                "password": "different-password",
            },
        },
        "exchange_whitelist": [
            {"exchange": "binance", "market": "future", "mode": "sandbox"},
            {"exchange": "kraken", "market": "future", "mode": "live"},
        ],
    }
    source = tmp_path / "config.json"
    source.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    target = tmp_path / "config.toml"
    expected = AppConfig.model_validate(raw)
    backup = migrate_config(source, target)
    assert not source.exists()
    assert json.loads(backup.read_text()) == raw
    actual = load_config(target, environ={})
    assert actual == expected
    assert list(actual.users) == list(expected.users)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert target.read_text().count("[[exchange_whitelist]]") == 2


def test_dotenv_migration_preserves_nested_fields_and_json_collections(
    tmp_path, monkeypatch
):
    source = tmp_path / ".env"
    source.write_text(
        """CCXT_PROXY_SECRET='file-secret'
CCXT_PROXY_USERS='{"Mixed.Case":{"password":"${UNCHANGED} # 密码"}}'
CCXT_PROXY_TQ__USERNAME='MixedCase'
CCXT_PROXY_TQ__PASSWORD='\\'quoted\\' \\\\ $HOME ${SECRET} #'
CCXT_PROXY_BINANCE__ENABLE_PROXY=false
CCXT_PROXY_BINANCE__TEST__API_KEY='key'
CCXT_PROXY_BINANCE__TEST__SECRET='secret'
CCXT_PROXY_EXCHANGE_WHITELIST='[{"exchange":"binance","market":"future","mode":"sandbox"}]'
CCXT_PROXY_TELEGRAM__BOT_TOKEN='test-token'
CCXT_PROXY_TELEGRAM__CHATS='{"Chat-A":"-123"}'
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CCXT_PROXY_SECRET", "process-secret")
    monkeypatch.setenv("UNCHANGED", "should-not-expand")
    expected = read_legacy_config(source)
    target = tmp_path / "config.toml"
    backup = migrate_config(source, target)
    actual = load_config(target, environ={})
    assert backup == tmp_path / ".env.bak"
    assert backup.exists() and not source.exists()
    assert actual == expected
    assert actual.SECRET == "file-secret"
    assert actual.users["Mixed.Case"].password == "${UNCHANGED} # 密码"
    assert (
        actual.tq is not None and actual.tq.password == "'quoted' \\ $HOME ${SECRET} #"
    )
    assert actual.telegram is not None and actual.telegram.chats == {"Chat-A": "-123"}


@pytest.mark.parametrize("conflict", ["target", "backup"])
def test_migration_does_not_overwrite_existing_files(tmp_path, conflict):
    source = tmp_path / "config.json"
    source.write_text('{"SECRET":"legacy"}')
    target = tmp_path / "config.toml"
    existing = target if conflict == "target" else tmp_path / "config.json.bak"
    existing.write_text("keep-existing")
    with pytest.raises(ConfigError, match="refusing to overwrite"):
        migrate_config(source, target)
    assert source.exists() and existing.read_text() == "keep-existing"


def test_migration_leaves_original_intact_on_failed_verification(tmp_path, monkeypatch):
    source = tmp_path / "config.json"
    source.write_text('{"SECRET":"legacy"}')
    target = tmp_path / "config.toml"
    monkeypatch.setattr(
        "scripts.migrate_config_to_toml.load_config",
        lambda *args, **kwargs: AppConfig(SECRET="different"),
    )
    with pytest.raises(ConfigError, match="verification"):
        migrate_config(source, target)
    assert source.exists() and not target.exists()


@pytest.mark.parametrize(
    "text",
    [
        "CCXT_PROXY_SECRET='do-not-disclose\n",
        "CCXT_PROXY_SECRET='first'\nCCXT_PROXY_SECRET='do-not-disclose'\n",
        "CCXT_PROXY_SECRET='valid'\nCCXT_PROXY_USERS='do-not-disclose'\n",
        "CCXT_PROXY_SECRET='valid'\nCCXT_PROXY_TQ__PASWORD='do-not-disclose'\n",
    ],
)
def test_migration_errors_keep_original_without_exposing_values(tmp_path, text):
    source = tmp_path / ".env"
    source.write_text(text)
    target = tmp_path / "config.toml"
    with pytest.raises(ConfigError) as caught:
        migrate_config(source, target)
    assert "do-not-disclose" not in str(caught.value)
    assert source.read_text() == text
    assert not target.exists()


def test_example_has_only_placeholder_credentials_and_valid_defaults():
    example = Path(__file__).resolve().parents[1] / "config.example.toml"
    config = load_config(example, environ={})
    assert config.SECRET == "replace-with-a-random-secret"
    assert config.users["admin"].password == "replace-with-your-password"
    assert config.exchange_whitelist == []
    assert all(
        getattr(config, provider) is None
        for provider in ("binance", "kraken", "tq", "ctp", "telegram")
    )


def test_bruno_reads_selected_toml_without_initializing_app(tmp_path, monkeypatch):
    from scripts import run_bruno

    path = write_toml(
        tmp_path,
        """SECRET = 'test'
[users.bruno-user]
password = 'bruno-pass'
""",
    )
    monkeypatch.setenv("CCXT_PROXY_CONFIG_PATH", str(path))
    monkeypatch.setattr("sys.argv", ["run_bruno.py", "Root.bru"])
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(run_bruno.subprocess, "run", run)
    assert run_bruno.main() == 0
    assert "user=bruno-user" in calls[0][0]
    assert "password=bruno-pass" in calls[0][0]


def test_render_preserves_empty_strings_and_legacy_proxy_fields(tmp_path):
    expected = AppConfig.model_validate(
        {
            "SECRET": "test",
            "tq": {"username": None, "password": ""},
            "proxy": {"__https": "http://localhost:8888"},
        }
    )
    assert (
        load_config(write_toml(tmp_path, render_toml(expected)), environ={}) == expected
    )
