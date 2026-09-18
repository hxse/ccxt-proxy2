import asyncio

import pytest
from fastapi import HTTPException

from src.tools.config_types import TqConfig
from src.tools.tq_client import TqClient
from src.tools.tq_status_snapshot import TqStatusSnapshot

SYMBOL = "SHFE.rb2610"


def notice(code):
    return {"notify": {str(code): {"conn_id": "ts", "code": code}}}


def status(raw="CONTINOUS"):
    return {"trading_status": {SYMBOL: {"trade_status": raw}}}


@pytest.mark.parametrize(
    ("raw", "is_open", "reason"),
    [
        ("CONTINOUS", True, None),
        ("AUCTIONORDERING", False, None),
        ("NOTRADING", False, None),
        ("NEW_STATUS", None, "unrecognized_status"),
        ("", None, "not_received"),
    ],
)
def test_status_snapshot_retains_mapping_and_deduplicates_subscription(
    raw, is_open, reason
):
    snapshot = TqStatusSnapshot()
    snapshot.activate(True)
    for _ in range(3):
        initial = snapshot.read(SYMBOL)
        assert initial.is_open is None and initial.reason == "not_received"
    assert snapshot.take_pending() == [SYMBOL]
    snapshot.observe([notice(2019112901), status(raw)])
    result = snapshot.read(SYMBOL)
    assert result.is_open is is_open and result.reason == reason
    assert result.raw_status == (raw or None)
    assert snapshot.take_pending() == []
    result.is_open = None  # 调用方修改响应不能污染共享快照。
    assert snapshot.read(SYMBOL).is_open is is_open


def test_permission_denial_is_immediate_and_does_not_schedule_subscription():
    snapshot = TqStatusSnapshot()
    snapshot.activate(False)
    with pytest.raises(HTTPException) as caught:
        snapshot.read(SYMBOL)
    assert caught.value.status_code == 403
    assert caught.value.detail == "TQ_TRADING_STATUS_PERMISSION_DENIED"
    assert snapshot.take_pending() == []


def test_disconnected_and_stopped_snapshot_never_returns_old_status():
    snapshot = TqStatusSnapshot()
    snapshot.activate(True)
    snapshot.observe([notice(2019112901), status()])
    assert snapshot.read(SYMBOL).is_open is True
    snapshot.observe([notice(2019112911)])
    assert snapshot.read(SYMBOL).reason == "disconnected"
    snapshot.observe([notice(2019112902)])
    assert snapshot.read(SYMBOL).reason == "not_received"
    snapshot.observe([status()])
    assert snapshot.read(SYMBOL).is_open is True
    snapshot.deactivate()
    snapshot.observe([status()])
    result = snapshot.read(SYMBOL)
    assert result.is_open is None and result.raw_status is None
    assert result.reason == "unavailable" and snapshot.take_pending() == []


def test_background_subscription_failure_is_reported_on_later_reads(tmp_path):
    client = TqClient(TqConfig(), lock_path=tmp_path / "tq.lock")
    client.status_snapshot.activate(True)
    assert client.status_snapshot.read(SYMBOL).reason == "not_received"

    class DeniedApi:
        def get_trading_status(self, symbol):
            raise Exception("您的账户不支持查看交易状态信息，需要购买后才能使用。")

    asyncio.run(client._subscribe_trading_status(DeniedApi(), SYMBOL))
    with pytest.raises(HTTPException) as caught:
        client.status_snapshot.read(SYMBOL)
    assert caught.value.status_code == 403
