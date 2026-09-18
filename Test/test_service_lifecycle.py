import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
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
            "tq": {"username": "test", "password": "test"},
            "ctp": ctp_config(tmp_path),
            "binance": {"test": {"api_key": "test", "secret": "test"}},
            "service_whitelist": [
                {
                    "service": "ccxt",
                    "exchange": "binance",
                    "market": "future",
                    "mode": "sandbox",
                },
                {"service": "tq"},
                {"service": "ctp", "mode": "sandbox"},
            ],
        }
    )


def fake_managers(events, fail=None):
    def manager(name):
        def initialize(*args):
            events.append("init:" + name)
            if fail == name:
                raise RuntimeError("offline initialization failed")

        return SimpleNamespace(
            initialize=initialize, close=lambda: events.append("close:" + name)
        )

    return [manager(name) for name in ("ccxt", "tq", "ctp")]


def test_all_whitelisted_services_start_once_in_order_and_close_in_reverse(tmp_path):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    managers = fake_managers(events)
    with pytest.raises(HTTPException) as pending:
        runtime.require("tq")
    assert pending.value.detail == {"code": "SERVICE_NOT_READY", "service": "tq"}
    runtime.start(*managers)
    runtime.start(*managers)
    assert runtime.ready and runtime.initialized == [
        "ccxt/binance/future/sandbox",
        "tq",
        "ctp/sandbox",
    ]
    runtime.require("tq")
    with pytest.raises(HTTPException) as disabled:
        runtime.require("ctp/live")
    assert disabled.value.detail == {
        "code": "SERVICE_NOT_ENABLED",
        "service": "ctp/live",
    }
    runtime.close()
    runtime.close()
    assert events == [
        "init:ccxt",
        "init:tq",
        "init:ctp",
        "close:ctp",
        "close:tq",
        "close:ccxt",
    ]
    assert not runtime.ready and runtime.initialized == []


@pytest.mark.parametrize("fail", ["ccxt", "tq", "ctp"])
def test_partial_startup_failure_cleans_up_and_never_becomes_ready(tmp_path, fail):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    with pytest.raises(RuntimeError):
        runtime.start(*fake_managers(events, fail))
    initialized = [
        event.removeprefix("init:") for event in events if event.startswith("init:")
    ]
    assert events[len(initialized) :] == [
        "close:" + name for name in reversed(initialized)
    ]
    assert not runtime.ready and runtime.initialized == []


def test_disabled_services_do_not_initialize_even_with_credentials_and_http_access(
    tmp_path, monkeypatch
):
    config = configured(tmp_path).model_copy(update={"service_whitelist": []})
    runtime = ServiceRuntime(config)
    events = []
    runtime.start(*fake_managers(events))
    assert runtime.ready and runtime.initialized == [] and events == []
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
        "src.tools.exchange_manager.DuckDbOhlcvCache",
        lambda *args: pytest.fail("disabled CCXT opened cache"),
    )
    ExchangeManager().init_from_config(config)


def test_ctp_is_authenticated_before_first_request_and_disabled_mode_stays_disabled(
    tmp_path,
):
    config = configured(tmp_path).model_copy(
        update={"service_whitelist": configured(tmp_path).service_whitelist[-1:]}
    )
    runtime = ServiceRuntime(config)
    factory = FakeFactory()
    ctp = CtpManager(config.ctp, factory, access_guard=runtime.require)
    try:
        runtime.start(None, None, ctp)
        assert [m for m, _, _ in factory.apis[0].requests] == [
            "ReqAuthenticate",
            "ReqUserLogin",
            "ReqSettlementInfoConfirm",
        ]
        client = ctp.get_client("sandbox")
        client.fetch_balance(CtpAccountQuery())
        assert sum(m == "ReqUserLogin" for m, _, _ in factory.apis[0].requests) == 1
        with pytest.raises(HTTPException):
            ctp.get_client("live")
        assert len(factory.apis) == 1
    finally:
        runtime.close()


def test_config_file_changes_do_not_change_running_whitelist_or_credentials(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        "SECRET='old'\n[tq]\nusername='old'\npassword='old'\n[[service_whitelist]]\nservice='tq'\n"
    )
    config = load_config(path)
    runtime = ServiceRuntime(config)
    manager = TqManager(config.tq)
    path.write_text("SECRET='new'\nservice_whitelist=[]\n")
    config.service_whitelist.clear()
    assert config.tq is not None
    config.tq.password = "changed in memory"
    runtime.start(*fake_managers([]))
    assert runtime.initialized == ["tq"]
    runtime.require("tq")
    assert manager._client._config is not None
    assert manager._client._config.password == "old"
    assert load_config(path).service_whitelist == []
    runtime.close()


def test_lifespan_waits_for_startup_and_always_releases_resources(
    tmp_path, monkeypatch
):
    runtime = ServiceRuntime(configured(tmp_path))
    events = []
    fake_ccxt, fake_tq, fake_ctp = fake_managers(events)
    entered, release = threading.Event(), threading.Event()
    original = fake_tq.initialize

    def initialize():
        entered.set()
        assert release.wait(3)
        original()

    fake_tq.initialize = initialize
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.shared.exchange_manager", fake_ccxt)
    monkeypatch.setattr("src.tools.tq_manager.tq_manager", fake_tq)
    monkeypatch.setattr("src.tools.ctp_manager.ctp_manager", fake_ctp)
    monkeypatch.setattr(
        "src.tools.telegram_manager.telegram_manager.close",
        lambda: events.append("close:telegram"),
    )
    app = FastAPI()

    async def run():
        async with lifespan(app):
            assert app.state.service_runtime.ready
            events.append("accept requests")

    with ThreadPoolExecutor(max_workers=1) as pool:
        done = pool.submit(asyncio.run, run())
        assert entered.wait(3) and not runtime.ready
        assert "accept requests" not in events
        release.set()
        done.result(timeout=5)
    assert events == [
        "init:ccxt",
        "init:tq",
        "init:ctp",
        "accept requests",
        "close:ctp",
        "close:tq",
        "close:ccxt",
        "close:telegram",
    ]
    assert not runtime.ready
