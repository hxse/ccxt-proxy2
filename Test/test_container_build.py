import subprocess
import tarfile

import pytest

from scripts import container_common as common
from scripts.container_source import BUILD_FILES, SourceInventory, prepare_source
from Test.helpers.container_engine import (
    CACHE_IMAGE,
    DEPENDENCY_IMAGE,
    NEW_IMAGE,
    OLD_IMAGE,
)
from Test.helpers.container_shell import ROOT, write_upload
from Test.helpers.container_shell import engine as engine
from Test.helpers.container_shell import source as source


def test_upload_then_remote_build_starts_without_any_existing_image(engine, source):
    state = engine.read()
    state.images.clear()
    engine.write(state)
    write_upload(source)
    result = engine.run("upload", source)
    assert result.returncode == 0, result.stderr
    assert not any(
        event[0] in {"build", "run", "start", "create"}
        for event in engine.read().events
    )
    assert common.read_prepared(engine.root) is None
    result = engine.run("build-start")
    assert result.returncode == 0, result.stderr
    state = engine.read()
    builds = [event for event in state.events if event[0] == "build"]
    assert len(builds) == 2 and "--target=dependencies" in builds[0]
    assert all(
        "--layers" in event and not any(arg.startswith("--platform") for arg in event)
        for event in builds
    )
    smoke = next(
        event for event in state.events if event[0] == "run" and "--tmpfs" in event
    )
    assert "--network=none" in smoke and "load_td_api" in smoke[-1]
    assert state.containers[common.NAME]["State"]["Running"]
    assert set(state.images) == {NEW_IMAGE, CACHE_IMAGE, DEPENDENCY_IMAGE}
    assert len(list((engine.root / ".container/sources").glob("*/manifest"))) == 1
    assert not list((engine.root / ".container").glob("context.*"))


@pytest.mark.parametrize(
    "failure, message",
    [
        ("fail_build", "build failed"),
        ("fail_smoke", "smoke failed"),
        ("invalid_config", "配置校验失败"),
    ],
)
def test_build_failure_preserves_old_prepared_and_running_instance(
    engine, source, failure, message
):
    engine.prepare(source, image=OLD_IMAGE)
    state = engine.read()
    identity = state.containers[common.NAME]["Config"]["Labels"][
        common.PREFIX + "configuration"
    ]
    prepared = engine.root / ".container/prepared"
    prepared.write_text(f"{OLD_IMAGE}\n{identity}\n")
    before = prepared.read_bytes()
    setattr(state, failure, True)
    engine.write(state)
    write_upload(source)
    result = engine.run("upload-build-start", source)
    assert result.returncode != 0 and message in result.stderr
    assert prepared.read_bytes() == before
    state = engine.read()
    assert state.containers[common.NAME]["Image"] == OLD_IMAGE
    assert state.containers[common.NAME]["State"]["Running"]
    assert not any(
        "build-build." in tag
        for item in state.images.values()
        for tag in item["RepoTags"]
    )


def test_cleanup_keeps_current_dependencies_external_and_active_ancestors(
    engine, source
):
    write_upload(source)
    result = engine.run("upload-build-start", source)
    assert result.returncode == 0, result.stderr
    assert "warning:" not in result.stderr
    state = engine.read()
    ids = {
        name: "sha256:" + char * 64
        for name, char in [
            ("old_parent", "e"),
            ("old_child", "f"),
            ("foreign", "1"),
            ("foreign_parent", "2"),
            ("active", "3"),
            ("active_parent", "4"),
        ]
    }
    for name, identity in ids.items():
        state.images[identity] = state.image(identity)
        if name.endswith(("parent", "child")):
            state.images[identity]["Labels"][common.PREFIX + "kind"] = "build-cache"
            state.images[identity]["Labels"].pop(common.PREFIX + "release")
    state.images[OLD_IMAGE] = state.image(OLD_IMAGE)
    state.images[OLD_IMAGE]["Parent"] = ids["old_child"]
    state.images[ids["old_child"]]["Parent"] = ids["old_parent"]
    state.images[ids["foreign"]]["Parent"] = ids["foreign_parent"]
    state.images[ids["foreign"]]["RepoTags"] = ["localhost/another-project:latest"]
    state.images[ids["active"]]["Parent"] = ids["active_parent"]
    state.container(
        "another-instance", ids["active"], engine.root / "other", "configuration"
    )
    state.events.clear()
    engine.write(state)
    result = subprocess.run(
        ["sh", str(ROOT / "scripts/container_cleanup.sh"), str(engine.root), NEW_IMAGE],
        env=engine.env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    state = engine.read()
    assert {OLD_IMAGE, ids["old_parent"], ids["old_child"]}.isdisjoint(state.images)
    assert {
        NEW_IMAGE,
        CACHE_IMAGE,
        DEPENDENCY_IMAGE,
        ids["foreign"],
        ids["foreign_parent"],
        ids["active"],
        ids["active_parent"],
    } <= state.images.keys()
    removed = [event[-1] for event in state.events if event[:2] == ("image", "rm")]
    assert (
        removed.index(OLD_IMAGE.removeprefix("sha256:"))
        < removed.index(ids["old_child"].removeprefix("sha256:"))
        < removed.index(ids["old_parent"].removeprefix("sha256:"))
    )
    assert not any("prune" in event for event in state.events)


def test_source_package_is_deterministic_and_excludes_private_data(tmp_path):
    root = tmp_path / "project"
    included = {
        *BUILD_FILES,
        "src/main.py",
        "src/tools/uncommitted.py",
        "src/openapi/cfb.json",
        "vendor/vnpy_ctp/test.tar.gz",
    }
    excluded = {
        "config.toml",
        "market_data.toml",
        ".env",
        "data/cache.duckdb",
        ".git/config",
        ".venv/lib/private.py",
        "src/__pycache__/main.pyc",
    }
    for name in included | excluded:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("private sentinel" if name in excluded else "source contents")
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    second.mkdir()
    prepare_source(root, first, SourceInventory("none", {}))
    prepare_source(root, second, SourceInventory("none", {}))
    assert (first / "source.delta.tar.gz").read_bytes() == (
        second / "source.delta.tar.gz"
    ).read_bytes()
    with tarfile.open(first / "source.delta.tar.gz") as archive:
        assert set(archive.getnames()) == included
        for member in archive:
            file = archive.extractfile(member)
            assert file is not None and b"private sentinel" not in file.read()
    (root / "src/leak.py").symlink_to(root / "config.toml")
    with pytest.raises(common.DeploymentError, match="普通文件"):
        prepare_source(root, second, SourceInventory("none", {}))


@pytest.mark.parametrize(
    "extra", ["../escaped.py", "config.toml", "src/../../escaped.py"]
)
def test_inner_source_archive_cannot_escape_or_include_runtime_configs(
    engine, source, extra
):
    write_upload(source, {extra: b"not source"})
    result = engine.run("upload", source)
    assert result.returncode != 0 and "源码清单" in result.stderr
    assert not (engine.root / ".container/uploaded").exists()
    assert not any(event[0] == "build" for event in engine.read().events)
