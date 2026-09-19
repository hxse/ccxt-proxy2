import pytest

from src.responses_ctp import CtpInstrumentStatus
from src.tools.ctp_manager import CtpManager
from src.types_ctp import CtpTradingStatusQuery
from Test.ctp_fakes import FakeFactory, ctp_config, record


@pytest.fixture
def status_client(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    manager.initialize("sandbox")
    yield manager.get_client("sandbox"), factory, manager
    manager.close()


def request(**values):
    return CtpTradingStatusQuery.model_validate(
        {"exchange_id": "SHFE", "product_id": "rb"} | values
    )


def status_record(status="2", exchange="SHFE", product="rb"):
    return record(
        CtpInstrumentStatus,
        ExchangeID=exchange,
        InstrumentID=product,
        ExchangeInstID=product,
        InstrumentStatus=status,
        EnterTime="09:00:00",
        EnterReason="1",
        TradingSegmentSN=1,
    )


@pytest.mark.parametrize("raw", list("01234567") + ["9", ""])
def test_ctp_status_mapping_retains_full_native_notification(status_client, raw):
    client, factory, _ = status_client
    first = client.fetch_trading_status(request())
    assert first.is_open is None and first.reason == "not_received"
    api = factory.apis[0]
    native = status_record(raw)
    api.callbacks.on_instrument_status(native)
    native["EnterTime"] = "00:00:00"  # 原生内存生命周期结束后不能影响快照。
    result = client.fetch_trading_status(request())

    assert result.raw_status == (raw or None)
    assert result.data is not None and result.data.EnterTime == "09:00:00"
    assert result.mode == "sandbox" and result.product_id == "rb"
    if raw in list("01234567"):
        assert result.is_open is (raw == "2")
        assert result.reason is None
    else:
        assert result.is_open is None and result.reason == "unrecognized_status"
    assert [method for method, _, _ in api.requests] == [
        "ReqAuthenticate",
        "ReqUserLogin",
        "ReqSettlementInfoConfirm",
    ]  # 后续读取不发送 ReqQry*，也不重复登录。


def test_ctp_status_is_latest_per_exchange_product_and_mode(status_client):
    client, factory, manager = status_client
    client.fetch_trading_status(request())
    manager.initialize("live")
    live = manager.get_client("live")
    live.fetch_trading_status(request(mode="live"))
    sandbox_api, live_api = factory.apis
    sandbox_api.callbacks.on_instrument_status(status_record("2"))
    sandbox_api.callbacks.on_instrument_status(status_record("6", "DCE"))
    sandbox_api.callbacks.on_instrument_status(status_record("1", product="cu"))
    live_api.callbacks.on_instrument_status(status_record("6"))

    assert client.fetch_trading_status(request()).is_open is True
    assert client.fetch_trading_status(request(exchange_id="DCE")).raw_status == "6"
    assert client.fetch_trading_status(request(product_id="cu")).raw_status == "1"
    assert (
        client.fetch_trading_status(request(product_id="rb2610")).reason
        == "not_received"
    )
    assert live.fetch_trading_status(request(mode="live")).is_open is False
    sandbox_api.callbacks.on_instrument_status(status_record("6"))
    assert client.fetch_trading_status(request()).raw_status == "6"


def test_ctp_disconnect_invalidates_snapshot_and_reconnect_requires_new_status(
    status_client,
):
    client, factory, _ = status_client
    client.fetch_trading_status(request())
    state = factory.apis[0].callbacks
    state.on_instrument_status(status_record())
    assert client.fetch_trading_status(request()).is_open is True
    state.on_disconnected()
    assert state.status_snapshot.read("SHFE", "rb")[1] is None
    state.on_instrument_status(status_record())  # 断线后的迟到通知不能恢复有效状态。
    assert state.status_snapshot.read("SHFE", "rb")[1] is None
    state.on_connected()
    result = client.fetch_trading_status(request())
    assert result.is_open is None and result.data is None
    assert result.reason == "not_received"
    state.on_instrument_status(status_record())
    assert client.fetch_trading_status(request()).is_open is True
    state.close()
    state.on_instrument_status(status_record())
    assert state.status_snapshot.read("SHFE", "rb")[1] is None


def test_ctp_status_never_initializes_or_reconnects(status_client, monkeypatch):
    client, factory, _ = status_client
    before = list(factory.apis[0].requests)
    monkeypatch.setattr(
        client, "_get_session", lambda: pytest.fail("status accessed SDK session")
    )
    factory.apis[0].callbacks.on_disconnected()
    result = client.fetch_trading_status(request())
    assert result.reason == "disconnected" and result.is_open is None
    factory.apis[0].callbacks.on_connected()
    assert client.fetch_trading_status(request()).reason == "not_received"
    assert factory.apis[0].requests == before


def test_ctp_status_without_startup_never_creates_client_or_sdk(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    assert manager.fetch_trading_status(request()).reason == "unavailable"
    assert factory.apis == [] and manager._clients == {}
    client = manager.get_client("sandbox")
    assert client.fetch_trading_status(request()).reason == "unavailable"
    assert factory.apis == []
    manager.close()
