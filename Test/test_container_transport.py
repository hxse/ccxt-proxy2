import hashlib
import io
import json
import multiprocessing
import shlex
import subprocess
import sys
import tarfile

import pytest

from scripts import container_common as common
from scripts import container_transport as transport
from src.tools.deployment_types import DeploymentConfig
from Test.helpers.container_shell import ROOT, source_archive, uploaded_snapshot
from Test.helpers.container_shell import engine as engine


def archive_bytes(members):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for member in members:
            if isinstance(member, str):
                info = tarfile.TarInfo(member)
                if member in transport.CONTROL_FILES:
                    content = (ROOT / member).read_bytes()
                elif member == "source.sha256":
                    content = (
                        hashlib.sha256(source_archive()).hexdigest() + "\n"
                    ).encode()
                elif member == "source.tar.gz":
                    content = source_archive()
                else:
                    content = b"image archive"
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
            else:
                archive.addfile(member)
    return stream.getvalue()


def receive(engine, content, action="upload", keep=False):
    receiver = (ROOT / "scripts/container_receive.sh").read_text()
    return subprocess.run(
        [
            "sh",
            "-c",
            receiver,
            "receive",
            engine.root.name,
            action,
            "0",
            str(keep).lower(),
        ],
        input=content,
        env=dict(
            engine.env, HOME=str(engine.root.parent), TMPDIR=str(engine.root.parent)
        ),
        capture_output=True,
        timeout=20,
    )


def test_receiver_control_runs_without_host_python_and_removes_temporary_files(engine):
    result = receive(engine, archive_bytes(transport.CONTROL_FILES), "generation")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"generation": 0}
    assert not engine.root.exists()
    assert not list(engine.root.parent.glob("tmp.*"))
    assert "python3" not in (ROOT / "scripts/container_receive.sh").read_text()


@pytest.mark.parametrize(
    "extra", ["../escaped", "/tmp/escaped", "config.toml", "unexpected.py", "link"]
)
def test_receiver_rejects_unknown_duplicate_and_link_members(engine, extra):
    member = extra
    if extra == "link":
        member = tarfile.TarInfo("config.toml")
        member.type = tarfile.SYMTYPE
        member.linkname = "../../escaped"
    content = archive_bytes(
        [member, *sorted(transport.CONTROL_FILES | transport.UPLOAD_FILES)]
    )
    result = receive(engine, content)
    assert result.returncode != 0
    assert "归档" in result.stderr.decode()
    assert not engine.root.exists()
    assert not engine.read().events


@pytest.mark.parametrize("keep", [False, True])
@pytest.mark.parametrize("configs", [set(), {"config.toml"}, set(common.CONFIG_FILES)])
def test_receiver_requires_configuration_files_matching_explicit_policy(
    engine, keep, configs
):
    members = transport.CONTROL_FILES | {"source.tar.gz", "source.sha256"} | configs
    result = receive(engine, archive_bytes(members), keep=keep)
    valid = not configs if keep else configs == set(common.CONFIG_FILES)
    if not valid:
        assert result.returncode != 0 and "归档" in result.stderr.decode()
        assert not engine.read().events
    elif keep:
        assert result.returncode != 0 and "没有准备版本" in result.stderr.decode()
    else:
        assert result.returncode == 0, result.stderr
        config = uploaded_snapshot(engine) / "config.toml"
        assert config.stat().st_mode & 0o777 == 0o600


def test_command_keeps_literal_argv_without_shell_evaluation(tmp_path):
    dangerous = f"'quoted path' $(touch {tmp_path / 'unexpected'})"
    output = common.command(
        [
            sys.executable,
            "-c",
            "import sys,json; print(json.dumps(sys.argv[1:]))",
            dangerous,
            "with spaces",
        ]
    )
    assert json.loads(output) == [dangerous, "with spaces"]
    assert not (tmp_path / "unexpected").exists()


def test_diagnostic_logs_include_stderr_and_command_failure_is_not_success():
    output = common.command(
        [
            sys.executable,
            "-c",
            "import sys; print('stdout'); print('stderr', file=sys.stderr)",
        ],
        merge_stderr=True,
    )
    assert "stdout" in output and "stderr" in output
    with pytest.raises(common.DeploymentError, match="退出码 7"):
        common.command([sys.executable, "-c", "raise SystemExit(7)"])


def _take_lock(root, waiting, acquired):
    waiting.set()
    with common.project_lock(root):
        acquired.set()


def test_project_lock_serializes_separate_processes(tmp_path):
    context = multiprocessing.get_context("spawn")
    waiting, acquired = context.Event(), context.Event()
    with common.project_lock(tmp_path):
        process = context.Process(target=_take_lock, args=(tmp_path, waiting, acquired))
        process.start()
        assert waiting.wait(3)
        assert not acquired.wait(0.3)
    try:
        assert acquired.wait(3)
    finally:
        process.join(timeout=3)
        if process.is_alive():
            process.terminate()
            process.join()
    assert process.exitcode == 0


@pytest.mark.parametrize("keep", [False, True])
def test_upload_uses_shell_and_only_sends_configs_when_requested(
    tmp_path, monkeypatch, keep
):
    configuration = (
        b'SECRET = "private-sentinel"\n[binance]\nenable_proxy = false\n'
        b"[kraken]\nenable_proxy = false\n"
    )
    (tmp_path / "config.toml").write_bytes(configuration)
    (tmp_path / "market_data.toml").write_bytes(b"private-sentinel")
    if keep:
        monkeypatch.setattr(
            transport,
            "prepare_remote_config",
            lambda *args: pytest.fail("patched preserved remote config"),
        )
    calls = []

    def ssh(args, **kwargs):
        assert args[0] == "ssh" and args[-2] == "rn"
        assert "BatchMode=yes" in args and "ConnectTimeout=10" in args
        parsed = shlex.split(args[-1])
        assert parsed[:2] == ["sh", "-c"]
        assert parsed[-4:] == [
            "dev/project's data $(literal)",
            "upload",
            "",
            str(keep).lower(),
        ]
        assert "private-sentinel" not in " ".join(args)
        with tarfile.open(fileobj=kwargs["stdin"], mode="r|gz") as archive:
            names = set()
            for member in archive:
                names.add(member.name)
                if member.name in common.CONFIG_FILES:
                    file = archive.extractfile(member)
                    expected = (
                        configuration.replace(b"false", b"true")
                        if member.name == "config.toml"
                        else b"private-sentinel"
                    )
                    assert file is not None and file.read() == expected
            expected = transport.CONTROL_FILES | transport.UPLOAD_FILES
            if keep:
                expected -= set(common.CONFIG_FILES)
            assert names == expected
        calls.append(args)

    monkeypatch.setattr(transport, "command", ssh)
    transport.request_remote(
        DeploymentConfig(ssh_host="rn", remote_dir="dev/project's data $(literal)"),
        "upload",
        source=None if keep else tmp_path,
        keep_remote_config=keep,
    )
    assert len(calls) == 1
    assert (tmp_path / "config.toml").read_bytes() == configuration


@pytest.mark.parametrize("action", ["generation", "start", "stop", "status", "logs"])
def test_control_transport_never_reads_image_or_config(monkeypatch, action):
    def ssh(args, **kwargs):
        with tarfile.open(fileobj=kwargs["stdin"], mode="r|gz") as archive:
            assert {member.name for member in archive} == transport.CONTROL_FILES
        assert (
            kwargs["timeout"] is None
            if action == "logs"
            else 0 < kwargs["timeout"] <= 240
        )
        return '{"generation": 1}'

    monkeypatch.setattr(transport, "command", ssh)
    assert (
        json.loads(
            transport.request_remote(
                DeploymentConfig(ssh_host="rn"), action, generation=1
            )
        )["generation"]
        == 1
    )


@pytest.mark.parametrize(
    "script",
    [
        "container_receive.sh",
        "container_manage.sh",
        "container_env.sh",
        "container_instance.sh",
        "container_source.sh",
        "container_build.sh",
        "container_cleanup.sh",
    ],
)
def test_deployment_scripts_are_valid_posix_shell(script):
    result = subprocess.run(
        ["sh", "-n", str(ROOT / "scripts" / script)], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
