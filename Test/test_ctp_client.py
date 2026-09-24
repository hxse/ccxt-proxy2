from typing import Any

import pytest

from src.tools.ctp_callbacks import CtpError, snapshot
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
from Test.ctp_fakes import QUERY_CASES, FakeFactory, Record, ctp_config, record

ORDER: dict[str, Any] = {
    "exchange_id": "SHFE",
    "instrument_id": "rb2610",
    "side": "buy",
    "offset": "open",
    "volume": 2,
}


@pytest.fixture
def service(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(ctp_config(tmp_path), factory)
    yield manager, factory
    manager.close()


def test_modes_have_separate_credentials_sessions_and_flow_directories(service):
    manager, factory = service
    for mode in ("sandbox", "live"):
        client = manager.get_client(mode)
        assert manager.get_client(mode) is client
        result = client.fetch_balance(CtpAccountQuery(mode=mode))
        assert result.mode == mode and result.accounts == []
        client.fetch_balance(CtpAccountQuery(mode=mode))
    test, live = factory.apis
    assert test.front.endswith(":10001") and live.front.endswith(":10002")
    assert test.flow.endswith("/sandbox/") and live.flow.endswith("/live/")
    assert [m for m, _, _ in test.requests[:3]] == [
        "ReqAuthenticate",
        "ReqUserLogin",
        "ReqSettlementInfoConfirm",
    ]
    assert [m for m, _, _ in live.requests[:2]] == [
        "ReqUserLogin",
        "ReqSettlementInfoConfirm",
    ]
    for api, password in ((test, "sim-password"), (live, "live-password")):
        logins = [
            fields for method, fields, _ in api.requests if method == "ReqUserLogin"
        ]
        assert len(logins) == 1 and logins[0]["Password"] == password


def test_unconfigured_mode_never_falls_back_to_other_account(tmp_path):
    factory = FakeFactory()
    manager = CtpManager(
        ctp_config(tmp_path).model_copy(update={"live": None}), factory
    )
    with pytest.raises(CtpError) as error:
        manager.get_client("live")
    assert (
        error.value.status_code == 503
        and error.value.detail["code"] == "CTP_NOT_CONFIGURED"
    )
    assert factory.apis == []


@pytest.mark.parametrize(
    ("stage", "released"), [("createFtdcTraderApi", 0), ("registerFront", 1)]
)
def test_initialization_failure_only_closes_a_created_native_api(
    tmp_path, stage, released
):
    factory = FakeFactory()

    def fail(*args):
        raise RuntimeError("initialization failed")

    factory.setup = lambda api: setattr(api, stage, fail)
    manager = CtpManager(ctp_config(tmp_path), factory)
    try:
        with pytest.raises(CtpError) as error:
            manager.get_client("sandbox").initialize()
        assert error.value.detail["code"] == "CTP_SDK_UNAVAILABLE"
        assert factory.apis[0].released == released
        assert factory.apis[0].initialized == bool(released)
        assert factory.apis[0].requests == []
    finally:
        manager.close()


@pytest.mark.parametrize(
    ("side", "offset", "direction", "flag"),
    [
        ("buy", "open", "0", "0"),
        ("sell", "open", "1", "0"),
        ("sell", "close_today", "1", "3"),
        ("buy", "close_yesterday", "0", "4"),
        ("sell", "close", "1", "1"),
    ],
)
def test_order_side_offset_and_integer_volume_are_forwarded(
    service, side, offset, direction, flag
):
    manager, factory = service
    request = CtpLimitOrderRequest.model_validate(
        ORDER | {"side": side, "offset": offset, "price": 3500}
    )
    client = manager.get_client("sandbox")
    result = client.create_order(request)
    assert result.order.Direction == direction and result.order.CombOffsetFlag == flag
    assert result.order.VolumeTotalOriginal == 2 and result.order.LimitPrice == 3500
    assert result.order.OrderRef == "11" and result.order.StatusMsg == "正常"
    assert result.trading_day == "20260917"
    assert client.create_order(request).order.OrderRef == "12"
    assert [method for method, _, _ in factory.apis[0].requests].count(
        "ReqOrderInsert"
    ) == 2
    methods = [method for method, _, _ in factory.apis[0].requests]
    assert methods.count("ReqQryInstrument") == 1
    assert methods.count("ReqQryDepthMarketData") == 2


@pytest.mark.parametrize(
    ("tif", "time_condition", "volume_condition"),
    [("GFD", "3", "1"), ("IOC", "1", "1"), ("FOK", "1", "3")],
)
def test_limit_time_in_force_is_native_mapping(
    service, tif, time_condition, volume_condition
):
    manager, factory = service
    manager.get_client("sandbox").create_order(
        CtpLimitOrderRequest(**ORDER, price=3500, time_in_force=tif)
    )
    fields = factory.apis[0].requests[-1][1]
    assert (
        fields["OrderPriceType"],
        fields["TimeCondition"],
        fields["VolumeCondition"],
    ) == ("2", time_condition, volume_condition)


def test_market_order_uses_native_anyprice_without_price_lookup(service):
    manager, factory = service
    manager.get_client("sandbox").create_order(CtpMarketOrderRequest(**ORDER))
    fields = factory.apis[0].requests[-1][1]
    assert (
        fields["OrderPriceType"],
        fields["LimitPrice"],
        fields["TimeCondition"],
        fields["VolumeCondition"],
    ) == ("1", 0, "1", "1")
    assert not any(
        method.startswith("ReqQry") for method, _, _ in factory.apis[0].requests
    )


@pytest.mark.parametrize(
    "request_data",
    [
        CtpCancelByExchange(
            by="exchange_order",
            exchange_id="SHFE",
            instrument_id="rb2610",
            order_sys_id="       42",
        ),
        CtpCancelBySession(
            by="session_order",
            exchange_id="SHFE",
            instrument_id="rb2610",
            front_id=1,
            session_id=-42,
            order_ref="55",
        ),
    ],
)
def test_cancel_uses_exact_original_identity(service, request_data):
    manager, factory = service
    result = manager.get_client("sandbox").cancel_order(request_data)
    fields = factory.apis[0].requests[-1][1]
    assert result.order.OrderStatus == "5" and fields["ActionFlag"] == "0"
    if isinstance(request_data, CtpCancelByExchange):
        assert fields["OrderSysID"] == "       42"
    else:
        assert (fields["FrontID"], fields["SessionID"], fields["OrderRef"]) == (
            1,
            -42,
            "55",
        )


@pytest.mark.parametrize(("name", "method", "key", "model"), QUERY_CASES)
def test_queries_aggregate_all_rows_without_coalescing(
    service, name, method, key, model
):
    manager, factory = service
    factory.setup = lambda api: api.queries.update(
        {method: [record(model), record(model)]}
    )
    requests = {
        "fetch_orders": CtpOrderQuery,
        "fetch_trades": CtpTradeQuery,
        "fetch_positions": CtpPositionQuery,
        "fetch_balance": CtpAccountQuery,
    }
    result = getattr(manager.get_client("sandbox"), name)(requests[name]())
    assert len(getattr(result, key)) == 2
    fields = factory.apis[0].requests[-1][1]
    assert fields["InvestorID"] == "sim-user" and fields["BrokerID"] == "9999"
    if name == "fetch_balance":
        assert fields["CurrencyID"] == "CNY" and fields["BizType"] == "1"


def test_json_conversion_preserves_identifiers_and_nulls_invalid_doubles():
    assert snapshot(
        Record(
            OrderSysID=b"    123",
            StatusMsg="正常".encode("gbk"),
            price=float("inf"),
            sentinel=1.7976931348623157e308,
            reserve1="unused",
        )
    ) == {"OrderSysID": "    123", "StatusMsg": "正常", "price": None, "sentinel": None}
