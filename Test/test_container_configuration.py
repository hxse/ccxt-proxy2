import subprocess
from pathlib import Path

import pytest
from pydantic import ValidationError

from scripts import container_cli as cli
from scripts import container_common as common
from src.tools.config_loader import ConfigError, load_config
from src.tools.deployment_types import DeploymentConfig

ROOT = Path(__file__).resolve().parents[1]


def test_deployment_is_optional_and_only_explicit_deploy_requires_it(tmp_path, capsys):
    config = tmp_path / "private.toml"
    config.write_text('SECRET = "private-marker"\n')
    assert load_config(config).deployment is None
    assert cli.main(["--target=remote", "--status", "--config", str(config)]) == 1
    assert "[deployment]" in capsys.readouterr().err
    config.write_text(config.read_text() + '[deployment]\nssh_host = "rn"\n')
    deployment = load_config(config).deployment
    assert deployment is not None
    assert deployment.remote_dir == "dev/ccxt-proxy2"


@pytest.mark.parametrize(
    "fields",
    [
        {"ssh_host": "ssh rn"},
        {"ssh_host": "-oProxyCommand=cmd"},
        {"ssh_host": True},
        {"ssh_host": "user@rn"},
        {"ssh_host": "rn", "unknown": True},
        {"ssh_host": "rn", "remote_dir": "/tmp/app"},
        {"ssh_host": "rn", "remote_dir": "../app"},
        {"ssh_host": "rn", "remote_dir": "dev/../app"},
        {"ssh_host": "rn", "remote_dir": "~/.app"},
        {"ssh_host": "rn", "remote_dir": "."},
        {"ssh_host": "rn", "remote_dir": ""},
        {"ssh_host": "rn", "remote_dir": "app\ncommand"},
    ],
)
def test_deployment_rejects_ambiguous_or_unsafe_settings(fields):
    with pytest.raises(ValidationError):
        DeploymentConfig.model_validate(fields)


def test_configuration_errors_hide_values(tmp_path):
    config = tmp_path / "private.toml"
    config.write_text(
        'SECRET = "PRIVATE_SENTINEL"\n[deployment]\nssh_host = "invalid secret host"\n'
    )
    with pytest.raises(ConfigError) as error:
        load_config(config)
    assert "deployment" in str(error.value)
    assert "PRIVATE_SENTINEL" not in str(error.value)
    assert "invalid secret host" not in str(error.value)


def test_space_and_quotes_in_remote_directory_are_preserved():
    value = "dev/my project's files $(literal)"
    assert DeploymentConfig(ssh_host="rn", remote_dir=value).remote_dir == value


@pytest.mark.parametrize("target, action", [("local", "start")])
def test_missing_image_never_triggers_implicit_build_or_ssh(
    tmp_path, monkeypatch, capsys, target, action
):
    config = tmp_path / "config.toml"
    config.write_text('SECRET = "hidden"\n[deployment]\nssh_host = "rn"\n')
    (tmp_path / "market_data.toml").write_text(
        "[tq_collection]\nenabled = false\n[retention]\nenabled = false\n"
    )
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "require_runtime", lambda: None)

    def missing(reference):
        raise common.DeploymentError(
            "镜像不存在，请先执行 just deploy --target=local --build"
        )

    def forbidden(*args, **kwargs):
        raise AssertionError("不得隐式构建、启动或上传")

    monkeypatch.setattr(cli, "inspect_image", missing)
    monkeypatch.setattr(cli, "build_image", forbidden)
    monkeypatch.setattr(cli, "activate", forbidden)
    monkeypatch.setattr(cli, "request_remote", forbidden)
    assert cli.main(["--target=" + target, "--" + action, "--config", str(config)]) == 1
    assert "先执行 just deploy --target=local --build" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args, code",
    [
        (["--help"], 0),
        (["--target=local", "--build", "--help"], 0),
        (["--target=remote", "--help"], 0),
        (["--target=local", "--start", "--unknown"], 2),
        (["--target=local", "--status", "--config=private.toml"], 2),
    ],
)
def test_help_and_invalid_arguments_have_no_side_effects(monkeypatch, args, code):
    def forbidden(*args, **kwargs):
        raise AssertionError("不应访问配置或 Podman")

    monkeypatch.setattr(cli, "require_runtime", forbidden)
    monkeypatch.setattr("src.tools.config_loader.load_config", forbidden)
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == code


def test_read_only_just_help_exposes_three_entries_and_old_entries_are_gone():
    result = subprocess.run(
        ["just", "--list"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    for name in ("deploy", "sync", "serve", "debug-cleanup-sandbox"):
        assert name in result.stdout
    assert "image-build" not in result.stdout
    assert "container-start" not in result.stdout
    assert "serve-ctp" not in result.stdout
    assert "docker-up-local" not in result.stdout
    assert "docker-down-local" not in result.stdout
    assert "docker-wait-ready" not in result.stdout
    assert not (ROOT / "docker-compose.yml").exists()
    assert not (ROOT / ".github/workflows/docker-image.yml").exists()


def test_image_configuration_stays_external_and_dependency_layers_precede_source():
    dockerfile = (ROOT / "Dockerfile").read_text()
    ignored = (ROOT / ".dockerignore").read_text().splitlines()
    assert {
        "config.toml",
        "config.toml.*",
        "market_data.toml",
        ".jj",
        ".container",
        "data",
    } <= set(ignored)
    copies = [line for line in dockerfile.splitlines() if line.startswith("COPY")]
    assert not any(
        "config.toml" in line or "market_data.toml" in line for line in copies
    )
    assert dockerfile.index("uv sync --locked") < dockerfile.index("COPY ./src/")
    assert "--mount=type=cache" in dockerfile
    assert '"--port", "5123"' in dockerfile
    assert "EXPOSE 8000" not in dockerfile
    assert dockerfile.rindex("LABEL io.ccxt-proxy2") > dockerfile.index("CMD [")


@pytest.mark.parametrize(
    "database, base_url, valid",
    [
        ("./data/cache/ohlcv.duckdb", "http://127.0.0.1:5123", True),
        ("/app/data/cache/ohlcv.duckdb", "http://127.0.0.1:5123", True),
        ("/tmp/cache.duckdb", "http://127.0.0.1:5123", False),
        ("data/../lost.duckdb", "http://127.0.0.1:5123", False),
        ("data/cache.duckdb", "http://127.0.0.1:8000", False),
    ],
)
def test_actual_preflight_checks_persistence_and_background_address(
    tmp_path, monkeypatch, database, base_url, valid
):
    config = tmp_path / "config.toml"
    config.write_text(
        'SECRET = "hidden"\n[users.admin]\npassword = "hidden"\n'
        f'[ohlcv_cache]\ndatabase_path = "{database}"\n'
        f'[market_data_client]\nuser = "admin"\nbase_url = "{base_url}"\n'
    )
    plan = tmp_path / "market_data.toml"
    plan.write_text("[tq_collection]\nenabled = false\n[retention]\nenabled = true\n")
    monkeypatch.setattr(
        "src.tools.config_loader.load_config", lambda: load_config(config)
    )
    monkeypatch.setattr("src.tools.market_data_config.PLAN_PATH", plan)
    if valid:
        exec(common.VALIDATE_CONFIG, {})
    else:
        with pytest.raises(SystemExit, match="容器配置无效"):
            exec(common.VALIDATE_CONFIG, {})
