"""用已安装 TqSdk 的真实消息循环检查断线与 diff 去重；不认证、不联网。"""

import logging
import time
from types import SimpleNamespace
from typing import Any

import pytest
from tqsdk import TqApi
from tqsdk.baseApi import TqBaseApi
from tqsdk.channel import TqChan
from tqsdk.entity import Entity
from tqsdk.objs import TradingStatus

from src.tools.config_types import TqConfig
from src.tools.tq_client import TqClient
from src.tools.tq_trading_status import TradingStatusTqApi

SYMBOL = "SHFE.rb2610"


@pytest.fixture
def sdk(monkeypatch):
    # 替换包含认证/联网的构造部分；使用实际 _fetch_msg、wait_update、_merge_diff。
    monkeypatch.setattr(TqApi, "__init__", lambda self: TqBaseApi.__init__(self))
    api: Any = TradingStatusTqApi()
    api._logger = logging.getLogger("offline.tq.status")
    api._serials = {}
    api._is_slave = True
    api._pending_diffs = []
    api._sync_diffs = []
    api._diffs = []
    api._data = Entity()
    api._data._instance_entity([])
    api._prototype = {"trading_status": {"#": TradingStatus(api)}}
    api._risk_manager = SimpleNamespace(_on_recv_data=lambda diffs: None)
    api._recv_chan = TqChan(api)
    api._pending_peek = False
    api._auth = SimpleNamespace(_has_feature=lambda feature: True)
    api._backtest = None
    api._requests = {"trading_status": set()}
    api.sent = []
    api._send_pack = api.sent.append
    api.status_snapshot.activate(api.has_trading_status_permission())
    yield api
    api._close_tasks()
    api._close_loop()


def feed(api, *diffs):
    api._recv_chan.send_nowait({"aid": "rtn_data", "data": list(diffs)})
    assert api.wait_update(deadline=time.time() + 0.5)


def notice(code, conn_id="ts"):
    return {"notify": {str(code): {"conn_id": conn_id, "code": code}}}


def status(value="CONTINOUS"):
    return {"trading_status": {SYMBOL: {"symbol": SYMBOL, "trade_status": value}}}


def test_same_status_after_reconnect_is_valid_even_when_sdk_removes_unchanged_diff(sdk):
    feed(sdk, notice(2019112901), status())
    assert sdk.status_snapshot.read(SYMBOL).is_open is True
    feed(sdk, notice(2019112911))
    assert sdk.status_snapshot.read(SYMBOL).reason == "disconnected"
    # SDK 本身仍保留旧值，新路由不能直接以这个缓存判定连续交易。
    assert sdk._data["trading_status"][SYMBOL].trade_status == "CONTINOUS"
    feed(sdk, notice(2019112902))
    assert sdk.status_snapshot.read(SYMBOL).reason == "not_received"
    feed(sdk, status())
    assert not any(SYMBOL in diff.get("trading_status", {}) for diff in sdk._diffs)
    # SDK 去除了值完全相同的重连回报，连接快照仍必须接受这次新回报。
    assert sdk.status_snapshot.read(SYMBOL).is_open is True


def test_only_ts_connection_controls_status_and_silence_is_not_closed(sdk):
    feed(sdk, notice(2019112901), status())
    feed(sdk, notice(2019112911, "md"))
    assert sdk.status_snapshot.read(SYMBOL).is_open is True
    assert sdk.wait_update(deadline=time.time() + 0.001) is False
    assert sdk.status_snapshot.read(SYMBOL).is_open is True


def test_batched_disconnect_reconnect_and_new_status_keep_packet_order(sdk):
    feed(sdk, notice(2019112901), status())
    feed(sdk, notice(2019112911), notice(2019112902), status("NOTRADING"))
    assert sdk.status_snapshot.read(SYMBOL).is_open is False
    feed(sdk, status(), notice(2019112911))
    assert sdk.status_snapshot.read(SYMBOL).reason == "disconnected"
    feed(sdk, status())  # 没有连接恢复通知，不接受迟到状态。
    assert sdk.status_snapshot.read(SYMBOL).reason == "disconnected"


def test_first_read_registers_one_subscription_and_sdk_does_not_enter_sync_wait(
    sdk, tmp_path, monkeypatch
):
    client = TqClient(
        TqConfig(), lock_path=tmp_path / "tq.lock", update_timeout_seconds=0
    )
    client._api = sdk
    client.status_snapshot = sdk.status_snapshot
    observed = []
    original = sdk.get_trading_status

    def subscribe(symbol):
        # 实际 TqApi 只有在协程里调用才会避开同步的 30 秒等待。
        assert sdk._loop.is_running()
        observed.append(symbol)
        return original(symbol)

    monkeypatch.setattr(sdk, "get_trading_status", subscribe)
    for _ in range(3):
        assert client.status_snapshot.read(SYMBOL).reason == "not_received"
    assert observed == []
    client.pump()
    assert observed == [SYMBOL]
    assert sdk.sent == [{"aid": "subscribe_trading_status", "ins_list": SYMBOL}]
    client.pump()
    assert observed == [SYMBOL]
    feed(sdk, notice(2019112901), status())
    assert client.status_snapshot.read(SYMBOL).is_open is True


def test_busy_pump_starts_subscription_and_handles_messages_without_network_wait(
    sdk, tmp_path, monkeypatch
):
    client = TqClient(TqConfig(), lock_path=tmp_path / "tq.lock")
    client._api = sdk
    client.status_snapshot = sdk.status_snapshot
    original_wait = sdk.wait_update

    def wait_update(*, deadline):
        assert deadline <= time.time()
        return original_wait(deadline=deadline)

    monkeypatch.setattr(sdk, "wait_update", wait_update)
    assert client.status_snapshot.read(SYMBOL).reason == "not_received"
    client.pump(wait=False)
    assert sdk.sent == [{"aid": "subscribe_trading_status", "ins_list": SYMBOL}]
    sdk._recv_chan.send_nowait(
        {"aid": "rtn_data", "data": [notice(2019112901), status()]}
    )
    client.pump(wait=False)
    assert client.status_snapshot.read(SYMBOL).is_open is True
    sdk._recv_chan.send_nowait({"aid": "rtn_data", "data": [notice(2019112911)]})
    client.pump(wait=False)
    assert client.status_snapshot.read(SYMBOL).reason == "disconnected"
