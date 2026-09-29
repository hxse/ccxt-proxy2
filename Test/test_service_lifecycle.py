import asyncio
import threading
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException

from src.tools.config_loader import load_config
from src.tools.config_types import AppConfig
from src.tools.ctp_manager import CtpManager
from src.tools.exchange_manager import ExchangeManager
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan
from src.tools.tq_manager import TqManager
from src.types_ctp import CtpAccountQuery
from src.types_tq import TqTickRequest
from Test.ctp_fakes import FakeFactory, ctp_config


def configured(tmp_path):
    return AppConfig.model_validate(
        {
            "SECRET": "offline",
            "ohlcv_cache": {"database_path": str(tmp_path / "cache.duckdb")},
            "tq": {"username": "test", "password": "test"},
            "ctp": ctp_config(tmp_path),
            "binance": {"test": {"api_key": "test", "secret": "test"}},
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    'is_live': False,
                },
                {"service": "tq"},
                {"service": "ctp", 'is_live': False},
            ],
        }
    )


def fake_managers(events, fail=None):
    def manager(name):
        def initialize(*args):
            events.append("init:" + name)
            if fail == name:
                raise RuntimeError("offline initialization failed")

        async def close_metadata():
            pass

        return SimpleNamespace(
            configure=lambda *args, **kwargs: None,
            initialize=initialize,
            is_ready=lambda *args: True,
            close=lambda: events.append("close:" + name),
            close_metadata=close_metadata,
        )

    return [manager(name) for name in ("ccxt", "tq", "ctp")]


def test_identities_start_once_and_close_in_reverse_ownership_order(tmp_path):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    managers = fake_managers(events)
    with pytest.raises(HTTPException) as pending:
        runtime.require("tq")
    assert pending.value.detail == {"code": "SERVICE_NOT_READY", "service": "tq"}
    runtime.start(*managers)
    runtime.wait_for_startup()
    runtime.start(*managers)
    assert runtime.ready and runtime.initialized == [
        "ccxt/binance/future/sandbox",
        "tq",
        "ctp/sandbox",
    ]
    assert sorted(events) == ["init:ccxt", "init:ctp", "init:tq"]
    runtime.require("tq")
    with pytest.raises(HTTPException) as disabled:
        runtime.require("ctp/live")
    assert disabled.value.detail == {
        "code": "SERVICE_NOT_ENABLED",
        "service": "ctp/live",
    }
    runtime.close()
    runtime.close()
    assert events[-3:] == ["close:ctp", "close:tq", "close:ccxt"]
    assert not runtime.ready and runtime.initialized == []


@pytest.mark.parametrize(
    "fail, identity",
    [("ccxt", "ccxt/binance/future/sandbox"), ("tq", "tq"), ("ctp", "ctp/sandbox")],
)
def test_initialization_failure_isolated_to_its_identity(tmp_path, fail, identity):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    try:
        runtime.start(*fake_managers(events, fail))
        runtime.wait_for_startup()
        assert runtime.ready
        assert sorted(events) == ["init:ccxt", "init:ctp", "init:tq"]
        assert runtime.snapshot()["services"][identity] == "failed"
        with pytest.raises(HTTPException) as error:
            runtime.require(identity)
        assert error.value.detail == {"code": "SERVICE_NOT_READY", "service": identity}
        for healthy in runtime.initialized:
            runtime.require(healthy)
        assert len(runtime.initialized) == 2
    finally:
        runtime.close()
    assert events[-3:] == ["close:ctp", "close:tq", "close:ccxt"]


def test_disabled_services_never_initialize_or_create_resources(tmp_path, monkeypatch):
    config = configured(tmp_path).model_copy(update={"service_whitelist": []})
    runtime = ServiceRuntime(config)
    events = []
    runtime.start(*fake_managers(events))
    assert runtime.snapshot() == {"status": "ready", "initialized": [], "services": {}}
    assert events == []
    tq = TqManager(
        config.tq,
        lock_path=tmp_path / "unused" / "tq.lock",
        access_guard=runtime.require,
    )
    factory = FakeFactory()
    ctp = CtpManager(config.ctp, factory, access_guard=runtime.require)
    with pytest.raises(HTTPException) as disabled_tq:
        tq.fetch_tick(TqTickRequest(symbol="SHFE.rb2610"))
    with pytest.raises(HTTPException) as disabled_ctp:
        ctp.get_client("sandbox")
    assert disabled_tq.value.detail == {"code": "SERVICE_NOT_ENABLED", "service": "tq"}
    assert disabled_ctp.value.detail == {
        "code": "SERVICE_NOT_ENABLED",
        "service": "ctp/sandbox",
    }
    assert not (tmp_path / "unused").exists() and factory.apis == []
    monkeypatch.setattr(
        "src.tools.cache_resource.DuckDbOhlcvCache",
        lambda *args: pytest.fail("disabled CCXT opened cache"),
    )
    ExchangeManager().init_from_config(config, None)
    runtime.close()


def test_ctp_authentication_still_precedes_client_access(tmp_path):
    config = configured(tmp_path)
    config.service_whitelist = config.service_whitelist[-1:]
    runtime = ServiceRuntime(config)
    factory = FakeFactory()
    ctp = CtpManager(config.ctp, factory, access_guard=runtime.require)
    try:
        runtime.start(None, None, ctp)
        runtime.wait_for_startup()
        assert [m for m, _, _ in factory.apis[0].requests] == [
            "ReqAuthenticate",
            "ReqUserLogin",
            "ReqSettlementInfoConfirm",
        ]
        ctp.get_client("sandbox").fetch_balance(CtpAccountQuery(is_live=False))
        assert sum(m == "ReqUserLogin" for m, _, _ in factory.apis[0].requests) == 1
        with pytest.raises(HTTPException):
            ctp.get_client("live")
        assert len(factory.apis) == 1
    finally:
        runtime.close()


def test_configuration_remains_a_startup_snapshot(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "SECRET='old'\n[tq]\nusername='old'\npassword='old'\n[[service_whitelist]]\nservice='tq'\n"
    )
    config = load_config(path)
    config.ohlcv_cache.database_path = str(tmp_path / "cache.duckdb")
    runtime = ServiceRuntime(config)
    manager = TqManager(config.tq)
    path.write_text("SECRET='new'\nservice_whitelist=[]\n")
    config.service_whitelist.clear()
    assert config.tq is not None
    config.tq.password = "changed in memory"
    try:
        runtime.start(*fake_managers([]))
        runtime.wait_for_startup()
        assert runtime.initialized == ["tq"]
        runtime.require("tq")
        assert manager._client._config is not None
        assert manager._client._config.password == "old"
        assert load_config(path).service_whitelist == []
    finally:
        runtime.close()


def test_lifespan_accepts_requests_while_tq_initialization_is_blocked(
    tmp_path, monkeypatch
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    ccxt, tq, ctp = fake_managers(events)
    entered, release = threading.Event(), threading.Event()
    original = tq.initialize

    def initialize(*args):
        entered.set()
        assert release.wait(3)
        original()

    tq.initialize = initialize
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", ccxt)
    monkeypatch.setattr("src.tools.tq_manager.tq_manager", tq)
    monkeypatch.setattr("src.tools.ctp_manager.ctp_manager", ctp)
    monkeypatch.setattr(
        "src.tools.telegram_manager.telegram_manager.close",
        lambda: events.append("close:telegram"),
    )

    async def run():
        async with lifespan(FastAPI()):
            assert await asyncio.to_thread(entered.wait, 2)
            assert runtime.ready
            assert runtime.snapshot()["services"]["tq"] == "initializing"
            with pytest.raises(HTTPException) as error:
                runtime.require("tq")
            assert error.value.detail == {"code": "SERVICE_NOT_READY", "service": "tq"}
            release.set()
            await asyncio.to_thread(runtime.wait_for_startup)
            runtime.require("tq")

    try:
        asyncio.run(run())
    finally:
        release.set()
    assert events[-4:] == ["close:ctp", "close:tq", "close:ccxt", "close:telegram"]
    assert not runtime.ready
