import io
import shutil
import tarfile

import pytest

from scripts.container_common import NAME
from scripts.container_source import BUILD_FILES, parse_inventory, prepare_source
from Test.helpers.container_engine import NEW_IMAGE
from Test.helpers.container_shell import engine as engine
from Test.helpers.container_shell import source as source


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "working-tree"
    for name in (
        *BUILD_FILES,
        "src/main.py",
        "src/old.py",
        "vendor/vnpy_ctp/test.tar.gz",
    ):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"original " + name.encode())
    return root


def inventory(engine):
    result = engine.run("inventory")
    assert result.returncode == 0, result.stderr
    return parse_inventory(result.stdout)


def version(engine):
    identity = (engine.root / ".container/uploaded").read_text().splitlines()[0]
    return engine.root / ".container/sources" / identity


def upload(engine, project, source):
    summary = prepare_source(project, source, inventory(engine))
    result = engine.run("upload", source)
    assert result.returncode == 0, result.stderr
    return summary


def test_first_unchanged_modified_and_renamed_inputs_are_exact(engine, project, source):
    first = upload(engine, project, source)
    assert first["changed"] == first["total"] > 0
    original_config = (source / "config.toml").read_bytes()
    data = engine.root / "data/keep.duckdb"
    data.parent.mkdir()
    data.write_bytes(b"database")
    assert upload(engine, project, source)["changed"] == 0
    with tarfile.open(source / "source.delta.tar.gz") as archive:
        assert archive.getnames() == []
    (project / "src/main.py").write_bytes(b"modified application")
    assert upload(engine, project, source)["changed"] == 1
    with tarfile.open(source / "source.delta.tar.gz") as archive:
        assert archive.getnames() == ["src/main.py"]
    (project / "src/old.py").rename(project / "src/new.py")
    assert upload(engine, project, source)["changed"] == 1
    assert not (version(engine) / "files/src/old.py").exists()
    assert (version(engine) / "files/src/new.py").read_bytes() == (
        project / "src/new.py"
    ).read_bytes()
    assert len(list((engine.root / ".container/sources").iterdir())) == 1
    assert data.read_bytes() == b"database"
    assert (source / "config.toml").read_bytes() == original_config
    assert not engine.read().containers


def test_remote_corrupted_and_missing_files_are_retransmitted(engine, project, source):
    upload(engine, project, source)
    current = version(engine)
    (current / "files/src/main.py").write_bytes(b"corrupt")
    (current / "files/src/old.py").unlink()
    summary = upload(engine, project, source)
    assert summary["changed"] == 2
    assert version(engine) == current
    assert (current / "files/src/main.py").read_bytes() == (
        project / "src/main.py"
    ).read_bytes()
    assert (current / "files/src/old.py").exists()
    assert not list((engine.root / ".container").glob("source-upload.*"))


def test_stale_source_baseline_does_not_replace_committed_version(
    engine, project, source, tmp_path
):
    upload(engine, project, source)
    before = inventory(engine)
    stale = tmp_path / "stale"
    stale.mkdir()
    for name in ("config.toml", "market_data.toml"):
        shutil.copyfile(source / name, stale / name)
    (project / "src/main.py").write_bytes(b"losing publisher")
    prepare_source(project, stale, before)
    (project / "src/main.py").write_bytes(b"winning publisher")
    upload(engine, project, source)
    committed = (engine.root / ".container/uploaded").read_bytes()
    result = engine.run("upload", stale)
    assert result.returncode != 0 and "版本已变化" in result.stderr
    assert (engine.root / ".container/uploaded").read_bytes() == committed
    assert (version(engine) / "files/src/main.py").read_bytes() == b"winning publisher"


@pytest.mark.parametrize("kind", ["bad_content", "symlink"])
def test_bad_delta_leaves_old_source_and_data_intact(engine, project, source, kind):
    upload(engine, project, source)
    committed = (engine.root / ".container/uploaded").read_bytes()
    (project / "src/main.py").write_bytes(b"new content")
    prepare_source(project, source, inventory(engine))
    with tarfile.open(source / "source.delta.tar.gz", "w:gz") as archive:
        member = tarfile.TarInfo("src/main.py")
        if kind == "symlink":
            member.type = tarfile.SYMTYPE
            member.linkname = "../../../config.toml"
            archive.addfile(member)
        else:
            member.size = len(b"wrong")
            archive.addfile(member, io.BytesIO(b"wrong"))
    result = engine.run("upload", source)
    expected = "内容校验失败" if kind == "bad_content" else "普通文件"
    assert result.returncode != 0 and expected in result.stderr
    assert (engine.root / ".container/uploaded").read_bytes() == committed
    assert (version(engine) / "files/src/main.py").read_bytes().startswith(b"original")
    assert not list((engine.root / ".container").glob("source-upload.*"))


def test_existing_full_archive_bootstraps_once_without_losing_config(
    engine, project, source
):
    upload(engine, project, source)
    config_id = (engine.root / ".container/uploaded").read_text().splitlines()[1]
    old_id = "e" * 64
    (engine.root / ".container/uploaded").write_text(old_id + "\n" + config_id + "\n")
    archive = engine.root / ".container/sources" / (old_id + ".tar.gz")
    archive.write_bytes(b"legacy source")
    before = inventory(engine)
    assert before.identity == old_id and before.hashes == {}
    summary = upload(engine, project, source)
    assert summary["changed"] == summary["total"]
    assert not archive.exists()
    assert upload(engine, project, source)["changed"] == 0


def test_local_mounts_original_files_and_applies_local_profile(engine, source):
    engine.root.mkdir()
    plan = engine.root / "market_data.toml"
    shutil.copyfile(source / "market_data.toml", plan)
    config = source / "config.toml"
    result = engine.run("activate", config, profile="local")
    assert result.returncode == 0, result.stderr
    state = engine.read()
    mounts = {item["Source"] for item in state.containers[NAME]["Mounts"]}
    assert mounts == {str(config), str(plan), str(engine.root / "data")}
    assert state.containers[NAME]["Config"]["Env"] == ["CCXT_PROXY_PROFILE=local"]
    assert not (engine.root / ".container/configs").exists()
    state.events.clear()
    engine.write(state)
    assert engine.run("activate", config, profile="local").returncode == 0
    assert not any(event[0] == "create" for event in engine.read().events)
    config.write_text('SECRET = "changed"\n')
    assert engine.run("activate", config, profile="local").returncode == 0
    assert any(event[0] == "create" for event in engine.read().events)


@pytest.mark.parametrize(
    "native, image_arch, valid",
    [("linux arm64", "arm64", True), ("linux amd64", "arm64", False)],
)
def test_image_must_match_actual_podman_platform(
    engine, source, native, image_arch, valid
):
    state = engine.read()
    state.native_platform = native
    state.images[NEW_IMAGE]["Architecture"] = image_arch
    engine.write(state)
    result = engine.run("activate", source)
    if valid:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0 and "原生平台" in result.stderr
        assert not engine.read().containers
