from pathlib import Path

import pytest

from scripts import container_common as common
from Test.helpers.container_engine import NEW_IMAGE, OLD_IMAGE
from Test.helpers.container_shell import engine as engine
from Test.helpers.container_shell import source as source


def test_first_start_and_repeated_start_reuse_one_loopback_instance(engine, source):
    engine.prepare(source)
    state = engine.read()
    create = next(event for event in state.events if event[0] == "create")
    assert create[create.index("--publish") + 1] == "127.0.0.1:5123:5123"
    assert f"{engine.root / 'data'}:/app/data:rw" in create
    assert "--pull=never" in create
    mount = next(arg for arg in create if arg.endswith(":/app/config.toml:ro"))
    assert Path(mount.split(":")[0]).stat().st_mode & 0o777 == 0o600
    state.events.clear()
    engine.write(state)
    engine.prepare(source)
    assert not any(
        event[0] in {"create", "start", "stop", "rm"} for event in engine.read().events
    )
    assert set(engine.read().containers) == {common.NAME}


def test_stopped_container_restarts_without_recreation(engine, source):
    engine.prepare(source)
    assert engine.run("stop").returncode == 0
    result = engine.run("activate", source, generation=1)
    assert result.returncode == 0, result.stderr
    assert len([event for event in engine.read().events if event[0] == "create"]) == 1
    assert engine.read().containers[common.NAME]["State"]["Running"]


@pytest.mark.parametrize("change", ["image", "configuration"])
def test_update_stops_old_first_and_keeps_database(engine, source, change):
    engine.prepare(source, image=OLD_IMAGE)
    database = engine.root / "data/ohlcv.duckdb"
    database.write_bytes(b"database")
    if change == "configuration":
        (source / "market_data.toml").write_text("# changed plan\n")
    state = engine.read()
    state.images[NEW_IMAGE] = state.image(NEW_IMAGE)
    state.events.clear()
    engine.write(state)
    engine.prepare(source, image=NEW_IMAGE if change == "image" else OLD_IMAGE)
    actions = [event[0] for event in engine.read().events]
    assert actions.index("stop") < actions.index("create") < actions.index("start")
    assert len(engine.read().containers) == 1
    assert database.read_bytes() == b"database"


@pytest.mark.parametrize("failure", ["configuration", "foreign", "readiness", "start"])
@pytest.mark.parametrize("running", [True, False])
def test_rejected_or_failed_update_preserves_old_instance(
    engine, source, failure, running
):
    engine.prepare(source, image=OLD_IMAGE)
    state = engine.read()
    state.images[NEW_IMAGE] = state.image(NEW_IMAGE)
    old = state.containers[common.NAME]
    old["State"]["Running"] = running
    if failure == "foreign":
        old["Config"]["Labels"][common.PREFIX + "directory"] = "/other"
    elif failure == "configuration":
        state.invalid_config = True
    elif failure == "readiness":
        state.unready_image = NEW_IMAGE
    else:
        state.fail_image = NEW_IMAGE
    state.events.clear()
    engine.write(state)
    result = engine.run("activate", source)
    assert result.returncode != 0
    expected = {
        "configuration": "配置校验失败",
        "foreign": "不属于本项目",
        "readiness": "已退出",
        "start": "start failed",
    }[failure]
    assert expected in result.stderr
    after = engine.read()
    assert set(after.containers) == {common.NAME}
    assert after.containers[common.NAME]["Image"] == OLD_IMAGE
    assert after.containers[common.NAME]["State"]["Running"] is running
    if failure in {"configuration", "foreign"}:
        assert not any(
            event[0] in {"stop", "rename", "create"} for event in after.events
        )
    else:
        log = engine.root / ".container/last-startup.log"
        assert "startup failure details" in log.read_text()
        assert log.stat().st_mode & 0o777 == 0o600


def test_port_conflict_never_kills_other_processes(engine, source):
    state = engine.read()
    state.busy_port = True
    engine.write(state)
    result = engine.run("activate", source)
    assert result.returncode != 0 and "port already bound" in result.stderr
    assert not engine.read().containers
    assert not any(event[0] == "kill" for event in engine.read().events)
