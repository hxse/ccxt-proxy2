import os
import subprocess
from pathlib import Path

import pytest

from scripts import serve

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def invoke_just(tmp_path):
    tools = tmp_path / "tools"
    tools.mkdir()
    capture = tmp_path / "argv"
    binary = tools / "uv"
    binary.write_text('#!/bin/sh\nprintf \'%s\\0\' "$@" > "$JUST_TEST_CAPTURE"\n')
    binary.chmod(0o700)

    def invoke(*args):
        capture.unlink(missing_ok=True)
        result = subprocess.run(
            ["just", *args],
            cwd=ROOT,
            env={
                **os.environ,
                "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                "JUST_TEST_CAPTURE": str(capture),
            },
            capture_output=True,
            text=True,
            timeout=10,
        )
        values = (
            capture.read_bytes().decode().rstrip("\0").split("\0")
            if capture.exists()
            else []
        )
        return result, values

    return invoke


def test_test_selectors_remain_single_arguments(invoke_just):
    result, values = invoke_just("test", "-k", "alpha or beta", "-q")
    assert result.returncode == 0, result.stderr
    assert values == [
        "run",
        "--no-sync",
        "pytest",
        "Test",
        "--ignore=Test/online",
        "--ignore=Test/cfb_native",
        "-k",
        "alpha or beta",
        "-q",
    ]
    result, values = invoke_just("test-file", "Test/a path.py", "-k", "alpha or beta")
    assert result.returncode == 0, result.stderr
    assert values == [
        "run",
        "--no-sync",
        "pytest",
        "Test/a path.py",
        "-k",
        "alpha or beta",
    ]
    result, values = invoke_just("test-online", "-k", "ohlcv or calendar")
    assert result.returncode == 0, result.stderr
    assert values[-2:] == ["-k", "ohlcv or calendar"]
    assert "Test/online/test_ccxt_online.py" in values


def test_sync_and_serve_have_separate_responsibilities(invoke_just):
    result, values = invoke_just("sync", "--extra=ctp")
    assert result.returncode == 0
    assert values == ["sync", "--locked", "--extra=ctp"]
    result, values = invoke_just(
        "serve", "--config=path with spaces.toml", "--port=5444"
    )
    assert result.returncode == 0
    assert values == [
        "run",
        "--no-sync",
        "python",
        "-m",
        "scripts.serve",
        "--config=path with spaces.toml",
        "--port=5444",
    ]


@pytest.mark.parametrize("extra", [[], ["--keep-remote-config"]])
def test_deploy_forwards_named_actions_without_implicit_subcommand(invoke_just, extra):
    result, values = invoke_just(
        "deploy", "--target=remote", "--upload", "--build", "--start", *extra
    )
    assert result.returncode == 0
    assert values == [
        "run",
        "--no-sync",
        "python",
        "-m",
        "scripts.container_cli",
        "--target=remote",
        "--upload",
        "--build",
        "--start",
        *extra,
    ]


def test_bruno_and_debug_arguments_are_not_shell_source(invoke_just, tmp_path):
    result, values = invoke_just("bru-run", "CCXT PROXY/error_contract")
    assert result.returncode == 0
    assert values[-1] == "CCXT PROXY/error_contract"
    unexpected = tmp_path / "should-not-exist"
    text = f"quoted 'text' $(touch {unexpected}) with spaces"
    result, values = invoke_just("debug-telegram-send", "scanner", text)
    assert result.returncode == 0
    assert values[-4:] == ["--chat", "scanner", "--text", text]
    result, values = invoke_just("debug-open-long", text)
    assert result.returncode == 0
    assert values[-3:] == ["open-long", "--amount", text]
    assert not unexpected.exists()


@pytest.mark.parametrize(
    "recipe", ["image-build", "container-start", "serve-ctp", "cleanup"]
)
def test_retired_recipes_do_not_run_any_tool(invoke_just, recipe):
    result, values = invoke_just(recipe)
    assert result.returncode != 0
    assert values == []


def test_serve_selects_config_and_execs_uvicorn_without_sync(tmp_path, monkeypatch):
    config = tmp_path / "a config.toml"
    config.write_text('SECRET = "offline-only"\n')
    monkeypatch.setenv("CCXT_PROXY_CONFIG_PATH", str(config))
    calls = []
    monkeypatch.setattr(
        serve.os, "execvp", lambda program, args: calls.append((program, args))
    )
    assert serve.main(["--config", str(config), "--host=127.0.0.1", "--port=5444"]) == 0
    assert calls == [
        (
            "uvicorn",
            [
                "uvicorn",
                "src.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "5444",
                "--reload",
            ],
        )
    ]
    assert os.environ["CCXT_PROXY_CONFIG_PATH"] == str(config)


@pytest.mark.parametrize(
    "args, code",
    [
        (["--help"], 0),
        (["--port=65536"], 2),
        (["--port=0"], 2),
        (["--host="], 2),
        (["127.0.0.1", "5123"], 2),
    ],
)
def test_serve_validates_before_reading_accounts(monkeypatch, args, code):
    def forbidden(*args, **kwargs):
        pytest.fail("help or invalid parameters read configuration")

    monkeypatch.setattr("src.tools.config_loader.load_config", forbidden)
    with pytest.raises(SystemExit) as error:
        serve.main(args)
    assert error.value.code == code
