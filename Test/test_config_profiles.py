import copy
from pathlib import Path

import pytest

from scripts import container_cli as cli
from src.tools.config_loader import ConfigError, load_config, load_deployment_config
from src.tools.config_profiles import apply_profile

TEXT = """SECRET = "private-sentinel"
service_whitelist = [{service="tq"}]
[proxy]
http = "http://proxy.example:3128"
[binance]
enable_proxy = false
[kraken]
enable_proxy = false
[tq]
enable_proxy = false
username = "offline"
password = "common-password"
[cfb]
enable_proxy = false
request_timeout_seconds = 10
[deployment]
ssh_host = "rn"
[overrides.remote.binance]
enable_proxy = true
[overrides.remote.kraken]
enable_proxy = true
[overrides.remote.tq]
enable_proxy = true
[overrides.remote.cfb]
enable_proxy = true
"""


@pytest.mark.parametrize("profile", ["dev", "local", "remote"])
def test_three_profiles_share_source_and_preserve_other_fields(tmp_path, profile):
    path = tmp_path / "config.toml"
    original = TEXT.replace("\n", "\r\n").encode()
    path.write_bytes(original)
    config = load_config(path, profile=profile, environ={})
    assert (
        config.tq is not None
        and config.binance is not None
        and config.kraken is not None
    )
    assert config.cfb is not None
    assert all(
        getattr(config, name).enable_proxy is (profile == "remote")
        for name in ("binance", "kraken", "tq")
    )
    assert config.cfb.enable_proxy is (profile == "remote")
    assert config.cfb.request_timeout_seconds == 10
    assert config.tq.password == "common-password"
    assert path.read_bytes() == original


@pytest.mark.parametrize("profile", [None, "", "remot", "production"])
def test_missing_or_unknown_profile_is_rejected_without_value(tmp_path, profile):
    path = tmp_path / "config.toml"
    path.write_text(TEXT)
    env = {} if profile is None else {"CCXT_PROXY_PROFILE": profile}
    with pytest.raises(ConfigError, match="must explicitly be") as caught:
        load_config(path, environ=env)
    assert "private-sentinel" not in str(caught.value)


def test_arrays_false_empty_values_and_source_ownership(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        TEXT
        + """
[overrides.local]
service_whitelist = []
[overrides.local.tq]
password = ""
enable_proxy = false
"""
    )
    config = load_config(path, environ={"CCXT_PROXY_PROFILE": "local"})
    assert config.service_whitelist == []
    assert config.tq is not None and config.tq.password == ""
    assert config.tq.enable_proxy is False
    raw = {
        "SECRET": "test",
        "tq": {"password": "old"},
        "overrides": {"remote": {"tq": {"password": ""}}},
    }
    original = copy.deepcopy(raw)
    merged = apply_profile(raw, "remote")
    merged["tq"]["password"] = "modified-result"
    assert raw == original


@pytest.mark.parametrize(
    "fragment",
    [
        '[overrides.remot]\nSECRET = "private-sentinel"\n',
        '[overrides.remote]\nunknown = "private-sentinel"\n',
        '[overrides.remote.tq]\npasword = "private-sentinel"\n',
        '[overrides.remote]\ntq = "private-sentinel"\n',
        "[overrides.remote]\noverrides = {}\n",
        "[overrides.remote.cfb]\nrequest_timeout_seconds = 0\n",
    ],
)
def test_unselected_bad_profile_also_fails_without_exposing_values(tmp_path, fragment):
    path = tmp_path / "config.toml"
    path.write_text('SECRET = "offline"\n' + fragment)
    with pytest.raises(ConfigError, match="Invalid overrides") as caught:
        load_config(path, profile="dev")
    assert "private-sentinel" not in str(caught.value)


def test_selected_profile_is_validated_after_merge(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(TEXT.replace('[proxy]\nhttp = "http://proxy.example:3128"\n', ""))
    assert load_config(path, profile="local").tq is not None
    with pytest.raises(ConfigError, match="Invalid TOML configuration"):
        load_config(path, profile="remote")


def test_deployment_uses_same_override_without_loading_accounts(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        '[deployment]\nssh_host = "local"\n[ctp.live]\npassword = "incomplete"\n[overrides.remote.deployment]\nssh_host = "rn"\n'
    )
    assert load_deployment_config(path, profile="remote").ssh_host == "rn"


def test_local_start_passes_original_file_without_copy(tmp_path, monkeypatch):
    config = tmp_path / "custom.toml"
    config.write_text(TEXT)
    (tmp_path / "market_data.toml").write_text(
        "[tq_collection]\nenabled=false\n[retention]\nenabled=false\n"
    )
    calls = []
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "require_runtime", lambda: None)
    monkeypatch.setattr(cli, "inspect_image", lambda image: {"Id": "a" * 64})
    monkeypatch.setattr(
        cli, "private_copy", lambda *a: pytest.fail("copied local configuration")
    )
    monkeypatch.setattr(
        cli, "activate", lambda root, image, source, **kw: calls.append(source)
    )
    assert cli.main(["--target=local", "--start", "--config", str(config)]) == 0
    assert calls == [config.resolve()]
    assert config.read_text() == TEXT
    assert not Path("scripts/container_proxy.py").exists()
