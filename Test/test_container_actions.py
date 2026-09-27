import json
import subprocess

import pytest

from scripts import container_cli as cli
from scripts import container_common as common
from Test.helpers.container_engine import NEW_IMAGE, OLD_IMAGE
from Test.helpers.container_shell import (
    engine as engine,
)
from Test.helpers.container_shell import (
    prepared_snapshot,
    uploaded_snapshot,
    write_upload,
)
from Test.helpers.container_shell import (
    source as source,
)


@pytest.mark.parametrize(
    "args",
    [
        ["--start"],
        ["--target=local"],
        ["--target=local", "--upload"],
        ["--target=remote", "--upload", "--start"],
        ["--target=local", "--start", "--stop"],
        ["--target=remote", "--logs", "--status"],
        ["--target=local", "--start", "--keep-remote-config"],
        ["--target=remote", "--start", "--keep-remote-config"],
        ["--target=remote", "--status", "--keep-remote-config"],
        ["build"],
        ["deploy"],
    ],
)
def test_invalid_combinations_rejected_before_any_config_or_command(monkeypatch, args):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid arguments caused side effects")

    monkeypatch.setattr(cli, "select_configuration", forbidden)
    with pytest.raises(SystemExit) as error:
        cli.main(args)
    assert error.value.code == 2


def test_bare_deploy_only_displays_help(monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "select_configuration", lambda args: pytest.fail("read config")
    )
    assert cli.main([]) == 0
    assert "--target" in capsys.readouterr().out


@pytest.mark.parametrize("action", ["build", "start", "stop", "status", "logs"])
def test_remote_control_needs_only_target_not_local_image_or_accounts(
    tmp_path, monkeypatch, action
):
    config = tmp_path / "target.toml"
    config.write_text(
        '[deployment]\nssh_host = "rn"\n[ctp.live]\npassword = "incomplete"\n'
    )
    events = []
    monkeypatch.setattr(
        cli, "require_runtime", lambda: pytest.fail("local Podman required")
    )
    monkeypatch.setattr(
        cli, "inspect_image", lambda image: pytest.fail("local image required")
    )
    monkeypatch.setattr(cli, "remote_generation", lambda target: 3)
    monkeypatch.setattr(
        cli,
        "request_remote",
        lambda target, action, **kwargs: events.append((action, kwargs)),
    )
    assert cli.main(["--target=remote", "--" + action, "--config", str(config)]) == 0
    assert events[0][0] == action
    assert "source" not in events[0][1] and "image" not in events[0][1]


def test_upload_preserving_config_reads_only_local_target(tmp_path, monkeypatch):
    config = tmp_path / "target.toml"
    config.write_text(
        '[deployment]\nssh_host = "rn"\n[ctp.live]\npassword = "incomplete"\n'
    )
    events = []
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(cli, "require_runtime", lambda: None)
    monkeypatch.setattr(cli, "inspect_image", lambda image: {"Id": NEW_IMAGE})
    monkeypatch.setattr(
        cli, "private_copy", lambda *args: pytest.fail("read local runtime config")
    )
    monkeypatch.setattr(
        cli,
        "request_remote",
        lambda target, action, **kwargs: events.append((action, kwargs)),
    )
    assert (
        cli.main(
            [
                "--target=remote",
                "--upload",
                "--keep-remote-config",
                "--config",
                str(config),
            ]
        )
        == 0
    )
    assert events == [
        (
            "upload",
            {
                "generation": None,
                "keep_remote_config": True,
            },
        )
    ]


def test_remote_pipeline_only_forwards_actions_and_never_builds_locally(
    tmp_path, monkeypatch
):
    config = tmp_path / "config.toml"
    config.write_text('SECRET = "test"\n[deployment]\nssh_host = "rn"\n')
    (tmp_path / "market_data.toml").write_text(
        "[tq_collection]\nenabled = false\n[retention]\nenabled = false\n"
    )
    events = []
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(
        cli, "require_runtime", lambda: pytest.fail("local Podman called")
    )
    monkeypatch.setattr(cli, "build_image", lambda: pytest.fail("local build called"))
    monkeypatch.setattr(
        cli, "remote_generation", lambda target: events.append("generation") or 0
    )
    monkeypatch.setattr(
        cli, "request_remote", lambda target, action, **kw: events.append(action)
    )
    args = [
        "--target=remote",
        "--start",
        "--build",
        "--upload",
        "--config",
        str(config),
    ]
    assert cli.main(args) == 0
    assert events == ["generation", "upload-build-start"]


def test_upload_preserves_running_instance_until_separate_start(engine, source):
    engine.prepare(source, image=OLD_IMAGE)
    write_upload(source)
    state = engine.read()
    state.images[NEW_IMAGE] = state.image(NEW_IMAGE)
    state.events.clear()
    engine.write(state)
    result = engine.run("upload", source)
    assert result.returncode == 0, result.stderr
    assert engine.read().containers[common.NAME]["Image"] == OLD_IMAGE
    assert not any(
        event[0] in {"start", "stop", "create", "rename"}
        for event in engine.read().events
    )
    snapshot = uploaded_snapshot(engine)
    original = (snapshot / "config.toml").read_bytes()
    (source / "config.toml").write_text("broken TOML [")
    result = engine.run("build-start")
    assert result.returncode == 0, result.stderr
    assert engine.read().containers[common.NAME]["Image"] == NEW_IMAGE
    assert (engine.root / "config.toml").read_bytes() == original


@pytest.mark.parametrize("keep", [False, True])
def test_upload_configuration_replacement_or_explicit_preservation(
    engine, source, keep
):
    write_upload(source)
    assert engine.run("upload", source).returncode == 0
    original = uploaded_snapshot(engine).name
    for name in common.CONFIG_FILES:
        if keep:
            (source / name).unlink()
            (engine.root / name).write_text("ignore manually edited copy")
        else:
            (source / name).write_text(f"# changed {name}\n")
    result = engine.run("upload-build-start", source, keep=keep)
    assert result.returncode == 0, result.stderr
    prepared = common.read_prepared(engine.root)
    assert prepared is not None
    assert (prepared["configuration"] == original) is keep
    for name in common.CONFIG_FILES:
        assert (engine.root / name).read_bytes() == (
            prepared_snapshot(engine) / name
        ).read_bytes()


@pytest.mark.parametrize("failure", ["missing", "checksum", "configuration"])
def test_bad_upload_or_first_keep_leaves_prepared_and_instance_untouched(
    engine, source, failure
):
    write_upload(source)
    if failure == "checksum":
        (source / "source.tar.gz").write_bytes(b"corrupt")
    elif failure == "configuration":
        state = engine.read()
        state.invalid_config = True
        engine.write(state)
    result = engine.run("upload-build-start", source, keep=failure == "missing")
    assert result.returncode != 0
    expected = {
        "missing": "没有准备版本",
        "checksum": "归档校验失败",
        "configuration": "配置校验失败",
    }
    assert expected[failure] in result.stderr
    assert common.read_prepared(engine.root) is None
    assert not engine.read().containers


def test_controls_bypass_operation_lock_and_cancel_queued_start(engine, source):
    write_upload(source)
    assert engine.run("upload-build", source).returncode == 0
    with common.project_lock(engine.root):
        process = subprocess.Popen(
            engine.command("start"),
            env=engine.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            assert process.stderr is not None
            assert "等待项目锁" in process.stderr.readline()
            status = engine.run("status")
            assert (
                status.returncode == 0
                and json.loads(status.stdout)["state"] == "absent"
            )
            assert engine.run("stop").returncode == 0
            _, error = process.communicate(timeout=5)
            assert process.returncode == 130 and "取消本次后续启动" in error
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    assert not engine.read().containers


def test_rootful_remote_podman_is_supported_without_changing_login(engine, source):
    state = engine.read()
    state.rootless = False
    engine.write(state)
    write_upload(source)
    result = engine.run("upload-build-start", source)
    assert result.returncode == 0, result.stderr
    assert engine.read().containers[common.NAME]["State"]["Running"]


def test_cleanup_failure_commits_prepared_but_cannot_start(engine, source):
    write_upload(source)
    state = engine.read()
    assert engine.run("upload", source).returncode == 0
    state = engine.read()
    state.fail_cleanup = True
    engine.write(state)
    result = engine.run("build-start")
    assert result.returncode != 0 and "新准备版本已保存" in result.stderr
    assert common.read_prepared(engine.root) is not None
    assert not engine.read().containers


def test_stop_during_update_cancels_start_and_keeps_old_instance_stopped(
    engine, source
):
    import time

    engine.prepare(source, image=OLD_IMAGE)
    state = engine.read()
    state.images[NEW_IMAGE] = state.image(NEW_IMAGE)
    state.hold_ready = True
    state.events.clear()
    engine.write(state)
    process = subprocess.Popen(
        engine.command("activate", source),
        env=engine.env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 8
        while not any(event[0] == "exec" for event in engine.read().events):
            assert time.monotonic() < deadline, "update did not reach readiness check"
            time.sleep(0.05)
        assert engine.run("stop").returncode == 0
        _, error = process.communicate(timeout=8)
        assert process.returncode == 130 and "取消本次后续启动" in error
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    after = engine.read()
    assert set(after.containers) == {common.NAME}
    assert after.containers[common.NAME]["Image"] == OLD_IMAGE
    assert not after.containers[common.NAME]["State"]["Running"]
    assert len([event for event in after.events if event[0] == "start"]) == 1
