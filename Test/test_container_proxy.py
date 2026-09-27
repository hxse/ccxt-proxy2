import copy
import tomllib

import pytest

from scripts import container_cli as cli
from scripts import container_transport as transport
from scripts.container_common import DeploymentError
from scripts.container_proxy import prepare_remote_config
from src.tools.config_loader import ConfigError
from src.tools.deployment_types import DeploymentConfig


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_remote_patch_only_changes_two_flags_and_preserves_source(tmp_path, newline):
    original = newline.join(
        [
            'SECRET = "private-sentinel"',
            "# enable_proxy = false 保留注释",
            "[proxy]",
            'http = "http://user:password@proxy.example:3128"',
            "[binance] # 交易所",
            "enable_proxy = false # 仅修改值",
            "[binance.live]",
            'api_key = "key"',
            'secret = "false $literal"',
            '["kraken"]',
            '"enable_proxy" = false',
            "",
        ]
    ).encode()
    source, target = tmp_path / "source.toml", tmp_path / "remote.toml"
    source.write_bytes(original)
    prepare_remote_config(source, target)
    patched = target.read_bytes()
    assert patched == original.replace(
        b"enable_proxy = false #", b"enable_proxy = true #"
    ).replace(b'"enable_proxy" = false', b'"enable_proxy" = true')
    assert source.read_bytes() == original
    assert target.stat().st_mode & 0o777 == 0o600
    again = tmp_path / "again.toml"
    prepare_remote_config(target, again)
    assert again.read_bytes() == patched


@pytest.mark.parametrize(
    "tables",
    [
        "",
        "[binance]",
        "[binance]\n# enable_proxy = false\n",
        '[binance.live]\napi_key = "key"\nsecret = "secret"\n',
        "[kraken]\nenable_proxy = true\n",
    ],
)
def test_optional_tables_and_missing_flags(tmp_path, tables):
    source, target = tmp_path / "source.toml", tmp_path / "remote.toml"
    source.write_text('SECRET = "private-sentinel"\n' + tables)
    expected = copy.deepcopy(tomllib.loads(source.read_text()))
    for exchange in ("binance", "kraken"):
        if exchange in expected:
            expected[exchange]["enable_proxy"] = True
    prepare_remote_config(source, target)
    assert tomllib.loads(target.read_text()) == expected


@pytest.mark.parametrize(
    "original, message",
    [
        ('SECRET = "private-sentinel"\n[binance', "无法解析 TOML"),
        (
            'SECRET = "private-sentinel"\n[binance]\nenable_proxy = "false"\n',
            "独立的布尔 enable_proxy",
        ),
        (
            'SECRET = """private-sentinel\n[binance]\nenable_proxy = false\n"""\n'
            '[binance.live]\napi_key = "key"\nsecret = "secret"\n',
            "目标开关以外",
        ),
    ],
)
def test_invalid_or_ambiguous_patch_is_rejected_without_changing_source(
    tmp_path, original, message
):
    source, target = tmp_path / "source.toml", tmp_path / "remote.toml"
    source.write_text(original)
    with pytest.raises(DeploymentError, match=message) as error:
        prepare_remote_config(source, target)
    assert "private-sentinel" not in str(error.value)
    assert source.read_text() == original
    assert not target.exists()


def test_remote_patch_requires_configured_proxy_before_sending(tmp_path, monkeypatch):
    original = (
        'SECRET = "private-sentinel"\n'
        'service_whitelist = [{service="ccxt", exchange="binance", '
        'market="future", mode="live"}]\n'
        "[binance]\nenable_proxy = false\n"
        '[binance.live]\napi_key = "key"\nsecret = "secret"\n'
    )
    source = tmp_path / "config.toml"
    source.write_text(original)
    monkeypatch.setattr(
        transport, "command", lambda *a, **kw: pytest.fail("sent invalid config")
    )
    with pytest.raises(ConfigError, match="Invalid TOML configuration") as error:
        transport.request_remote(
            DeploymentConfig(ssh_host="rn"), "upload", source=tmp_path
        )
    assert "private-sentinel" not in str(error.value)
    assert source.read_text() == original


def test_local_start_uses_original_flags_without_remote_patch(tmp_path, monkeypatch):
    original = b'SECRET = "private-sentinel"\n[binance]\nenable_proxy = false\n'
    config = tmp_path / "config.toml"
    config.write_bytes(original)
    (tmp_path / "market_data.toml").write_text(
        "[tq_collection]\nenabled = false\n[retention]\nenabled = false\n"
    )
    calls = []
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "require_runtime", lambda: None)
    monkeypatch.setattr(cli, "inspect_image", lambda image: {"Id": "a" * 64})
    monkeypatch.setattr(
        transport,
        "prepare_remote_config",
        lambda *a: pytest.fail("patched local config"),
    )

    def activate(root, image, source, *, guard):
        calls.append((source / "config.toml").read_bytes())

    monkeypatch.setattr(cli, "activate", activate)
    assert cli.main(["--target=local", "--start", "--config", str(config)]) == 0
    assert calls == [original]
    assert config.read_bytes() == original
