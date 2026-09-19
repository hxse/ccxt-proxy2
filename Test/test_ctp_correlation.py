"""私有流回报可以跨会话；只有明确属于本次操作的错误才能结束 HTTP 请求。"""

from pathlib import Path

import pytest

from src.ctp_records_trading import CtpOrder
from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_client import CtpClient
from src.tools.ctp_manager import CtpManager
from src.types_ctp import CtpCancelByExchange, CtpCancelBySession, CtpLimitOrderRequest
from Test.ctp_fakes import FakeFactory, Record, ctp_config, record

ORDER = CtpLimitOrderRequest(
    exchange_id="SHFE",
    instrument_id="rb2610",
    side="buy",
    offset="open",
    volume=1,
    price=3500,
)
CANCEL_EXCHANGE = CtpCancelByExchange(
    by="exchange_order",
    exchange_id="SHFE",
    instrument_id="rb2610",
    order_sys_id="       42",
)
CANCEL_SESSION = CtpCancelBySession(
    by="session_order",
    exchange_id="SHFE",
    instrument_id="rb2610",
    front_id=7,
    session_id=9,
    order_ref="11",
)


@pytest.fixture
def service(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    yield manager, factory
    manager.close()


def complete_order(api, values, method):
    fields = {
        key: value for key, value in values.items() if key in CtpOrder.model_fields
    }
    fields.setdefault("FrontID", 7)
    fields.setdefault("SessionID", api.session_id)
    fields.setdefault("OrderRef", "11")
    fields.setdefault("OrderSysID", "       42")
    api.callbacks.on_order(
        record(
            CtpOrder,
            **(
                fields
                | {
                    "OrderStatus": "3" if method == "ReqOrderInsert" else "5",
                    "OrderSubmitStatus": "3",
                }
            ),
        )
    )


@pytest.mark.parametrize("operation", [ORDER, CANCEL_EXCHANGE, CANCEL_SESSION])
@pytest.mark.parametrize(
    "change",
    [
        {"InstrumentID": "ag2612"},
        {"BrokerID": "other"},
        {"InvestorID": "another"},
        {"OrderMemo": "other-client"},
        {"OrderMemo": ""},
        {"RequestID": 999},
    ],
)
def test_unrelated_error_does_not_reject_a_successful_operation(
    service, operation, change
):
    assert_unrelated_error_is_ignored(service, operation, change)


def assert_unrelated_error_is_ignored(service, operation, change):
    manager, factory = service
    method = "ReqOrderInsert" if operation is ORDER else "ReqOrderAction"

    def setup(api):
        def respond(data, rid):
            values = data.copy()
            api.callbacks.on_error_return(
                method,
                Record(**(values | change)),
                Record(ErrorID=31, ErrorMsg="unrelated rejection"),
            )
            assert not api.callbacks.pending.event.is_set()
            complete_order(api, values, method)
            return 0

        api.hooks[method] = respond

    factory.setup = setup
    client = manager.get_client("sandbox")
    result = (
        client.create_order(operation)
        if operation is ORDER
        else client.cancel_order(operation)
    )
    assert result.order.OrderStatus == ("3" if operation is ORDER else "5")


@pytest.mark.parametrize(
    ("operation", "change"),
    [
        (ORDER, {"OrderRef": "999"}),
        (ORDER, {"ExchangeID": "DCE"}),
        (CANCEL_EXCHANGE, {"OrderSysID": "       99"}),
        (CANCEL_EXCHANGE, {"OrderActionRef": 999}),
        (CANCEL_SESSION, {"SessionID": 19}),
        (CANCEL_SESSION, {"FrontID": 19}),
        (CANCEL_SESSION, {"OrderRef": "99"}),
    ],
)
def test_correct_memo_still_requires_the_original_order_identity(
    service, operation, change
):
    assert_unrelated_error_is_ignored(service, operation, change)


@pytest.mark.parametrize("operation", [ORDER, CANCEL_EXCHANGE, CANCEL_SESSION])
@pytest.mark.parametrize("request_id_present", [True, False])
def test_matching_error_preserves_upstream_details(
    service, operation, request_id_present
):
    manager, factory = service
    method = "ReqOrderInsert" if operation is ORDER else "ReqOrderAction"

    def setup(api):
        def respond(data, rid):
            values = data.copy()
            if not request_id_present:
                values["RequestID"] = 0
            api.callbacks.on_error_return(
                method,
                Record(**values),
                Record(ErrorID=31, ErrorMsg="拒绝 sim-password"),
            )
            return 0

        api.hooks[method] = respond

    factory.setup = setup
    with pytest.raises(CtpError) as caught:
        client = manager.get_client("sandbox")
        client.create_order(operation) if operation is ORDER else client.cancel_order(
            operation
        )
    assert caught.value.detail["ctp_error_id"] == 31
    assert caught.value.detail["message"] == "拒绝 ***"
    assert caught.value.status_code == (422 if operation is ORDER else 409)


def test_repeated_cancel_uses_a_new_memo_and_ignores_late_rejection(service):
    manager, factory = service
    previous = []

    def setup(api):
        def respond(data, rid):
            values = data.copy()
            if previous:
                # 模拟旧回报甚至与当前请求号相同；旧 memo 仍必须被拒绝。
                stale = previous[-1] | {"RequestID": rid, "OrderActionRef": rid}
                api.callbacks.on_error_return(
                    "ReqOrderAction",
                    Record(**stale),
                    Record(ErrorID=31, ErrorMsg="late rejection"),
                )
                # 同一订单的历史撤单拒绝通知，也不能结束新的撤单请求。
                fields = {k: v for k, v in stale.items() if k in CtpOrder.model_fields}
                api.callbacks.on_order(
                    record(
                        CtpOrder,
                        **(fields | {"OrderStatus": "3", "OrderSubmitStatus": "5"}),
                    )
                )
                assert not api.callbacks.pending.event.is_set()
            previous.append(values)
            complete_order(api, values, "ReqOrderAction")
            return 0

        api.hooks["ReqOrderAction"] = respond

    factory.setup = setup
    client = manager.get_client("sandbox")
    client.cancel_order(CANCEL_EXCHANGE)
    client.cancel_order(CANCEL_EXCHANGE)
    memos = [item["OrderMemo"] for item in previous]
    assert len(set(memos)) == 2
    assert all(len(memo.encode("ascii")) == 12 for memo in memos)


def test_missing_error_memo_leaves_status_unknown_instead_of_claiming_rejection(
    tmp_path: Path,
):
    factory = FakeFactory()

    def setup(api):
        def respond(data, rid):
            values = data.copy() | {"OrderMemo": ""}
            api.callbacks.on_error_return(
                "ReqOrderInsert",
                Record(**values),
                Record(ErrorID=31, ErrorMsg="unattributed rejection"),
            )
            return 0

        api.hooks["ReqOrderInsert"] = respond

    factory.setup = setup
    config = ctp_config(tmp_path).model_copy(update={"request_timeout_seconds": 0.01})
    manager = CtpManager(config, factory)
    try:
        with pytest.raises(CtpError) as caught:
            manager.get_client("sandbox").create_order(ORDER)
        assert caught.value.detail["code"] == "OPERATION_STATUS_UNKNOWN"
    finally:
        manager.close()


def test_same_account_sessions_with_identical_request_and_order_refs_do_not_cross(
    tmp_path,
):
    config = ctp_config(tmp_path)
    assert config.test is not None
    factory = FakeFactory()
    previous = []

    def setup(api):
        api.session_id = 10 + len(factory.apis)

        def respond(data, rid):
            values = data.copy()
            if previous:
                other = previous[-1]
                assert values["RequestID"] == other["RequestID"]
                assert values["OrderRef"] == other["OrderRef"]
                assert values["OrderMemo"] != other["OrderMemo"]
                api.callbacks.on_error_return(
                    "ReqOrderInsert",
                    Record(**other),
                    Record(ErrorID=31, ErrorMsg="other session"),
                )
                assert not api.callbacks.pending.event.is_set()
            previous.append(values)
            complete_order(api, values, "ReqOrderInsert")
            return 0

        api.hooks["ReqOrderInsert"] = respond

    factory.setup = setup
    first = CtpClient(config.test, config, "sandbox", factory)
    second = CtpClient(config.test, config, "sandbox", factory)
    try:
        assert first.create_order(ORDER).order.SessionID == 11
        assert second.create_order(ORDER).order.SessionID == 12
    finally:
        first.close()
        second.close()
