"""原生 SDK 的离线边界检查；网络回归只连接本机假 TCP 前置。"""

import ctypes
import importlib.util
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.ctp_records_account import CtpPosition, CtpTradingAccount
from src.ctp_records_trading import CtpOrder, CtpTrade
from src.tools.ctp_callbacks import CtpCallbacks, CtpError, snapshot
from src.tools.ctp_spi import create_api
from Test.ctp_fakes import ctp_config


def test_sdk_factory_rejects_an_unpatched_installation(tmp_path, monkeypatch):
    monkeypatch.setitem(
        sys.modules, "ctpwrapper", SimpleNamespace(__version__="6.7.13")
    )
    config = ctp_config(tmp_path)
    assert config.test is not None
    callbacks = CtpCallbacks(config.test, "sandbox")
    with pytest.raises(CtpError, match="CTP_SDK_UNAVAILABLE") as caught:
        create_api(callbacks)
    assert caught.value.status_code == 503
    assert "uv sync --locked --extra ctp" in caught.value.detail["message"]


@pytest.mark.skipif(
    importlib.util.find_spec("ctpwrapper") is None,
    reason="optional ctp extra is not installed",
)
@pytest.mark.parametrize(
    "scenario", ["callback_waiting_for_gil", "callback_in_progress"]
)
def test_native_release_finishes_while_a_disconnect_callback_needs_the_gil(scenario):
    try:
        result = subprocess.run(
            [sys.executable, "-m", "Test.ctp_release_probe", scenario],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
            timeout=12,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("CTP Release deadlocked with a native callback waiting for the GIL")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "released" in result.stdout


@pytest.mark.skipif(
    importlib.util.find_spec("ctpwrapper") is None,
    reason="optional ctp extra is not installed",
)
def test_documented_records_cover_all_native_fields():
    from ctpwrapper import ApiStructure

    for native_name, model in [
        ("OrderField", CtpOrder),
        ("TradeField", CtpTrade),
        ("InvestorPositionField", CtpPosition),
        ("TradingAccountField", CtpTradingAccount),
    ]:
        native_type = getattr(ApiStructure, native_name)
        native = native_type.from_buffer_copy(bytes(ctypes.sizeof(native_type)))
        data = snapshot(native)
        assert set(model.model_fields) == set(data)
        model.model_validate(data)


@pytest.mark.skipif(
    importlib.util.find_spec("ctpwrapper") is None,
    reason="optional ctp extra is not installed",
)
def test_native_sdk_initialization_and_release_without_any_front():
    # 子进程和超时隔离原生释放错误，不能让离线 suite 卡在 C++ 线程中。
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import tempfile
from ctpwrapper import TraderApiPy
with tempfile.TemporaryDirectory() as path:
    api = TraderApiPy()
    api.Create(path + '/', True)
    api.SubscribePrivateTopic(2, 0)
    api.SubscribePublicTopic(2)
    api.Init()
    api.Release()
print('released')
""",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
    assert "released" in result.stdout


@pytest.mark.skipif(
    importlib.util.find_spec("ctpwrapper") is None,
    reason="optional ctp extra is not installed",
)
def test_forwarded_parameters_construct_real_sdk_structures(tmp_path):
    from ctpwrapper import ApiStructure

    from src.tools.ctp_manager import CtpManager
    from src.types_ctp import (
        CtpAccountQuery,
        CtpCancelByExchange,
        CtpCancelBySession,
        CtpLimitOrderRequest,
        CtpMarketOrderRequest,
        CtpOrderQuery,
        CtpPositionQuery,
        CtpTradeQuery,
    )
    from Test.ctp_fakes import FakeFactory, ctp_config

    fake = FakeFactory()

    def factory(callbacks):
        api, _ = fake(callbacks)
        return api, ApiStructure

    manager = CtpManager(ctp_config(tmp_path), factory)
    client = manager.get_client("sandbox")
    values = {
        "exchange_id": "SHFE",
        "instrument_id": "rb2610",
        "side": "buy",
        "offset": "open",
        "volume": 1,
    }
    try:
        client.create_order(CtpMarketOrderRequest.model_validate(values))
        client.create_order(
            CtpLimitOrderRequest.model_validate(values | {"price": 3500})
        )
        client.cancel_order(
            CtpCancelByExchange(
                by="exchange_order",
                exchange_id="SHFE",
                instrument_id="rb2610",
                order_sys_id="       42",
            )
        )
        client.cancel_order(
            CtpCancelBySession(
                by="session_order",
                exchange_id="SHFE",
                instrument_id="rb2610",
                front_id=7,
                session_id=9,
                order_ref="11",
            )
        )
        client.fetch_orders(CtpOrderQuery())
        client.fetch_trades(CtpTradeQuery())
        client.fetch_positions(CtpPositionQuery())
        client.fetch_balance(CtpAccountQuery())
    finally:
        manager.close()
