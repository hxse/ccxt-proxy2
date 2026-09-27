"""执行真实部署 Shell，只替换 Podman 进程，持久化命令/回执协议。"""

import fcntl
import gzip
import hashlib
import io
import json
import os
import shlex
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from scripts import container_common as common
from scripts.container_source import BUILD_FILES
from Test.helpers.container_engine import NEW_IMAGE, ContainerEngine

ROOT = Path(__file__).resolve().parents[2]


class ShellEngine:
    def __init__(self, folder):
        self.root = folder / "project with spaces"
        self.state = folder / "engine.json"
        self.write(ContainerEngine())
        executable = folder / "bin/podman"
        executable.parent.mkdir()
        executable.write_text(
            "#!/bin/sh\nexec "
            + shlex.join([sys.executable, "-m", "Test.helpers.container_engine"])
            + ' "$@"\n'
        )
        executable.chmod(0o700)
        self.env = dict(
            os.environ,
            PATH=str(executable.parent) + ":" + os.environ["PATH"],
            PYTHONPATH=str(ROOT),
            FAKE_PODMAN_STATE=str(self.state),
        )

    def read(self):
        engine = ContainerEngine()
        with self.state.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_SH)
            engine.__dict__.update(json.loads(self.state.read_text()))
        engine.events = [tuple(event) for event in engine.events]
        return engine

    def write(self, engine):
        self.state.write_text(json.dumps(engine.__dict__))

    def command(self, action, source="", image=None, generation=0, keep=False):
        return [
            "sh",
            str(ROOT / "scripts/container_manage.sh"),
            str(self.root),
            action,
            str(source),
            str(generation),
            str(keep).lower(),
            image or (NEW_IMAGE if action == "activate" else ""),
        ]

    def run(self, action, source="", *, image=None, generation=0, keep=False):
        return subprocess.run(
            self.command(action, source, image, generation, keep),
            env=self.env,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def prepare(self, source, *, image=NEW_IMAGE):
        result = self.run("activate", source, image=image)
        assert result.returncode == 0, result.stderr


@pytest.fixture
def engine(tmp_path):
    return ShellEngine(tmp_path)


@pytest.fixture
def source(tmp_path):
    folder = tmp_path / "input"
    folder.mkdir()
    (folder / "config.toml").write_text('SECRET = "private-value"\n')
    (folder / "market_data.toml").write_text(
        "[tq_collection]\nenabled = false\n[retention]\nenabled = false\n"
    )
    return folder


def source_archive(extra=None):
    files = dict.fromkeys(BUILD_FILES, b"build fixture")
    files.update(
        {
            "src/main.py": b"# application",
            "vendor/vnpy_ctp/test.tar.gz": b"vendor source",
        }
    )
    files.update(extra or {})
    stream = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as compressed,
        tarfile.open(fileobj=compressed, mode="w") as archive,
    ):
        for name, content in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)
            archive.addfile(member, io.BytesIO(content))
    return stream.getvalue()


def write_upload(source, extra=None):
    archive = source_archive(extra)
    (source / "source.tar.gz").write_bytes(archive)
    (source / "source.sha256").write_text(hashlib.sha256(archive).hexdigest() + "\n")


def uploaded_snapshot(engine):
    identity = (engine.root / ".container/uploaded").read_text().splitlines()[1]
    return engine.root / ".container/configs" / identity


def prepared_snapshot(engine):
    prepared = common.read_prepared(engine.root)
    assert prepared is not None
    return engine.root / ".container/configs" / prepared["configuration"]
