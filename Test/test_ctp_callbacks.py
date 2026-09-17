import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest

from src.ctp_records_trading import CtpOrder
from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_manager import CtpManager
from src.types_ctp import (
    CtpAccountQuery,
    CtpCancelByExchange,
    CtpLimitOrderRequest,
    CtpOrderQuery,
)
from Test.ctp_fakes import FakeFactory, Record, ctp_config, record

ORDER: dict[str, Any] = {
    "exchange_id": "SHFE",
    "instrument_id": "rb2610",
    "side": "buy",
    "offset": "open",
    "volume": 1,
    "price": 3500,
}


@pytest.fixture
def service(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    yield manager, factory
    manager.close()


def test_copy_native_memory_ignore_wrong_request_and_wait_for_last(service):
    manager, factory = service

    def setup(api):
        def respond(data, rid):
            row = record(CtpOrder, InstrumentID="rb2610", LimitPrice=3500)
            api.callbacks.on_response("ReqQryOrder", row, None, rid + 1, True)
            api.callbacks.on_response("ReqQryTrade", row, None, rid, True)
            api.callbacks.on_response("ReqQryOrder", row, None, rid, False)
            assert not api.callbacks.pending.event.is_set()
            row.values["LimitPrice"] = 9999
            api.callbacks.on_response("ReqQryOrder", None, None, rid, True)
            return 0

        api.hooks["ReqQryOrder"] = respond

    factory.setup = setup
    result = manager.get_client("sandbox").fetch_orders(CtpOrderQuery())
    assert len(result.orders) == 1 and result.orders[0].LimitPrice == 3500


def test_query_error_discards_partial_results_and_redacts_credentials(service):
    manager, factory = service

    def setup(api):
        def respond(data, rid):
            api.callbacks.on_response("ReqQryOrder", record(CtpOrder), None, rid, False)
            api.callbacks.on_response(
                "ReqQryOrder",
                None,
                Record(ErrorID=7, ErrorMsg="拒绝 sim-password test-auth"),
                rid,
                True,
            )
            return 0

        api.hooks["ReqQryOrder"] = respond

    factory.setup = setup
    with pytest.raises(CtpError) as caught:
        manager.get_client("sandbox").fetch_orders(CtpOrderQuery())
    assert caught.value.detail["ctp_error_id"] == 7
    assert caught.value.detail["message"] == "拒绝 *** ***"


@pytest.mark.parametrize(
    "channel",
    ["response", "error_return", "error_without_request_id", "order", "generic"],
)
def test_all_insert_rejection_channels_are_reported_without_retry(service, channel):
    manager, factory = service

    def setup(api):
        def reject(data, rid):
            error = Record(ErrorID=31, ErrorMsg="不支持此报单")
            if channel == "response":
                api.callbacks.on_response("ReqOrderInsert", data, error, rid, True)
            elif channel.startswith("error"):
                if channel == "error_without_request_id":
                    data.values["RequestID"] = 0
                api.callbacks.on_error_return("ReqOrderInsert", data, error)
            elif channel == "generic":
                api.callbacks.on_response("OnRspError", None, error, rid, True)
            else:
                api.callbacks.on_order(
                    Record(
                        BrokerID=data.values["BrokerID"],
                        InvestorID=data.values["InvestorID"],
                        InstrumentID="rb2610",
                        FrontID=7,
                        SessionID=9,
                        OrderRef=data.values["OrderRef"],
                        OrderStatus="5",
                        OrderSubmitStatus="4",
                        StatusMsg="拒单",
                    )
                )
            return 0

        api.hooks["ReqOrderInsert"] = reject

    factory.setup = setup
    with pytest.raises(CtpError) as caught:
        manager.get_client("sandbox").create_order(CtpLimitOrderRequest(**ORDER))
    assert caught.value.status_code == 422
    assert caught.value.detail["code"] == "CTP_ORDER_REJECTED"
    assert caught.value.detail["order_identity"]["order_ref"] == "11"
    assert (
        sum(method == "ReqOrderInsert" for method, _, _ in factory.apis[0].requests)
        == 1
    )


@pytest.mark.parametrize("action", ["insert", "cancel", "query"])
def test_timeout_never_returns_partial_success_and_next_request_rebuilds_session(
    tmp_path, action
):
    factory = FakeFactory()
    config = ctp_config(tmp_path).model_copy(update={"request_timeout_seconds": 0.01})
    manager = CtpManager(config, factory)
    method = {
        "insert": "ReqOrderInsert",
        "cancel": "ReqOrderAction",
        "query": "ReqQryOrder",
    }[action]

    def setup(api):
        def respond(data, rid):
            api.callbacks.on_response(
                method, None, None, rid, True if action != "query" else False
            )
            return 0

        api.hooks[method] = respond

    factory.setup = setup
    client = manager.get_client("sandbox")
    try:
        with pytest.raises(CtpError) as caught:
            if action == "insert":
                client.create_order(CtpLimitOrderRequest(**ORDER))
            elif action == "cancel":
                client.cancel_order(
                    CtpCancelByExchange(
                        by="exchange_order",
                        exchange_id="SHFE",
                        instrument_id="rb2610",
                        order_sys_id="   42",
                    )
                )
            else:
                client.fetch_orders(CtpOrderQuery())
        assert caught.value.detail["code"] == (
            "CTP_TIMEOUT" if action == "query" else "OPERATION_STATUS_UNKNOWN"
        )
        if action != "query":
            assert caught.value.detail["order_identity"]["instrument_id"] == "rb2610"
        factory.setup = lambda api: None
        assert client.fetch_orders(CtpOrderQuery()).orders == []
        assert len(factory.apis) == 2 and factory.apis[0].released == 1
        # 已释放连接的迟到回报不能完成新连接的请求。
        assert factory.apis[0].callbacks.pending is None
    finally:
        manager.close()


@pytest.mark.parametrize(("result", "status"), [(-1, 503), (-2, 429), (-3, 429)])
def test_nonzero_send_result_is_reported_once(service, result, status):
    manager, factory = service
    factory.setup = lambda api: api.hooks.update(
        {"ReqOrderInsert": lambda data, rid: result}
    )
    with pytest.raises(CtpError) as caught:
        manager.get_client("sandbox").create_order(CtpLimitOrderRequest(**ORDER))
    assert caught.value.status_code == status
    assert caught.value.detail["return_code"] == result
    assert caught.value.detail["code"] == "CTP_SEND_FAILED"
    assert (
        sum(method == "ReqOrderInsert" for method, _, _ in factory.apis[0].requests)
        == 1
    )


def test_unrelated_and_initial_submitted_order_notifications_are_ignored(service):
    manager, factory = service

    def setup(api):
        def respond(data, rid):
            fields = {
                "BrokerID": data.values["BrokerID"],
                "InvestorID": data.values["InvestorID"],
                "InstrumentID": "rb2610",
                "ExchangeID": "SHFE",
                "FrontID": 7,
                "SessionID": 9,
                "OrderRef": data.values["OrderRef"],
            }
            api.callbacks.on_order(
                record(
                    CtpOrder,
                    **(
                        fields
                        | {
                            "SessionID": 999,
                            "OrderStatus": "3",
                            "OrderSubmitStatus": "3",
                        }
                    ),
                )
            )
            api.callbacks.on_order(
                record(CtpOrder, **fields, OrderStatus="a", OrderSubmitStatus="0")
            )
            assert not api.callbacks.pending.event.is_set()
            api.callbacks.on_order(
                record(CtpOrder, **fields, OrderStatus="3", OrderSubmitStatus="3")
            )
            return 0

        api.hooks["ReqOrderInsert"] = respond

    factory.setup = setup
    assert (
        manager.get_client("sandbox")
        .create_order(CtpLimitOrderRequest(**ORDER))
        .order.SessionID
        == 9
    )


def test_disconnect_during_write_is_unknown_and_reconnect_logs_in_again(service):
    manager, factory = service

    def setup(api):
        def disconnect(data, rid):
            api.callbacks.on_disconnected()
            return 0

        api.hooks["ReqOrderInsert"] = disconnect

    factory.setup = setup
    client = manager.get_client("sandbox")
    with pytest.raises(CtpError) as caught:
        client.create_order(CtpLimitOrderRequest(**ORDER))
    assert caught.value.detail["code"] == "OPERATION_STATUS_UNKNOWN"
    api = factory.apis[0]
    api.session_id = 19
    api.callbacks.on_connected()
    client.fetch_balance(CtpAccountQuery())
    assert sum(m == "ReqUserLogin" for m, _, _ in api.requests) == 2


def test_query_pacing_and_serial_execution(service, monkeypatch):
    manager, factory = service
    client = manager.get_client("sandbox")
    client.fetch_orders(CtpOrderQuery())
    client._session.config = client._session.config.model_copy(
        update={"query_interval_seconds": 1.1}
    )
    sleeps = []
    monkeypatch.setattr("src.tools.ctp_session.time.monotonic", lambda: 100)
    monkeypatch.setattr("src.tools.ctp_session.time.sleep", sleeps.append)
    client._session._next_query_at = 0
    client.fetch_orders(CtpOrderQuery())
    client.fetch_orders(CtpOrderQuery())
    assert sleeps == pytest.approx([1.1])


def test_close_waits_for_active_request_and_closed_client_cannot_reopen(service):
    manager, factory = service
    sent = threading.Event()
    factory.setup = lambda api: api.hooks.update(
        {"ReqQryOrder": lambda data, rid: (sent.set(), 0)[1]}
    )
    client = manager.get_client("sandbox")
    with ThreadPoolExecutor(max_workers=2) as executor:
        read = executor.submit(client.fetch_orders, CtpOrderQuery())
        assert sent.wait(1)
        close = executor.submit(client.close)
        assert not close.done()
        api = factory.apis[0]
        api.callbacks.on_response("ReqQryOrder", None, None, api.requests[-1][2], True)
        assert read.result(timeout=1).orders == []
        close.result(timeout=1)
    client.close()
    assert api.released == 1
    with pytest.raises(CtpError) as error:
        client.fetch_orders(CtpOrderQuery())
    assert error.value.detail["code"] == "CTP_CLIENT_CLOSED"
