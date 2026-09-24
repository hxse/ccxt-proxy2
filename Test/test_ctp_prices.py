import pytest

from src.tools.ctp_callbacks import CtpError
from src.tools.ctp_client import CtpClient
from src.types_ctp import CtpLimitOrderRequest, CtpMarketOrderRequest
from Test.ctp_fakes import FakeFactory, ctp_config

BASE = {
    "mode": "sandbox",
    "exchange_id": "DCE",
    "instrument_id": "m2701",
    "side": "buy",
    "offset": "open",
    "volume": 1,
}


@pytest.fixture
def price_service(tmp_path):
    factory = FakeFactory()
    config = ctp_config(tmp_path)
    client = CtpClient(config.test, config, "sandbox", factory)
    client.initialize()
    yield client, factory.apis[0]
    client.close()


def insert_count(api):
    return sum(method == "ReqOrderInsert" for method, _, _ in api.requests)


@pytest.mark.parametrize(("side", "expected"), [("buy", 3574), ("sell", 3575)])
def test_ctp_normalizes_with_real_query_callbacks(price_service, side, expected):
    client, api = price_service
    result = client.create_order(
        CtpLimitOrderRequest(**(BASE | {"side": side}), price=3574.2)
    )
    assert result.order.LimitPrice == expected
    assert result.price_adjustment.submitted_price == str(expected)
    assert result.price_adjustment.requested_price == "3574.2"
    assert [method for method, _, _ in api.requests][-3:] == [
        "ReqQryInstrument",
        "ReqQryDepthMarketData",
        "ReqOrderInsert",
    ]


def test_instrument_prefix_results_are_filtered_exactly(price_service):
    client, api = price_service
    api.queries["ReqQryInstrument"] = [
        {"InstrumentID": "m2701-C-3000", "ExchangeID": "DCE", "PriceTick": 0.5},
        {"InstrumentID": "m2701", "ExchangeID": "DCE", "PriceTick": 1},
    ]
    result = client.create_order(CtpLimitOrderRequest(**BASE, price=3574.8))
    assert result.order.LimitPrice == 3574


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [
            {"InstrumentID": "m2701", "ExchangeID": "SHFE", "PriceTick": 1},
        ],
        [
            {"InstrumentID": "m2701", "ExchangeID": "DCE", "PriceTick": 1},
            {"InstrumentID": "m2701", "ExchangeID": "DCE", "PriceTick": 2},
        ],
        [{"InstrumentID": "m2701", "ExchangeID": "DCE", "PriceTick": 0}],
    ],
)
def test_missing_wrong_ambiguous_or_invalid_contract_does_not_trade(
    price_service, rows
):
    client, api = price_service
    api.queries["ReqQryInstrument"] = rows
    with pytest.raises(CtpError) as error:
        client.create_order(CtpLimitOrderRequest(**BASE, price=3574))
    assert error.value.status_code == 503
    assert error.value.detail["code"] == "PRICE_RULES_UNAVAILABLE"
    assert error.value.detail["order_identity"] is None and insert_count(api) == 0


def test_depth_rejects_out_of_bounds_without_changing_order_ref(price_service):
    client, api = price_service
    api.queries["ReqQryDepthMarketData"] = [
        {
            "InstrumentID": "m2701",
            "ExchangeID": "DCE",
            "TradingDay": "20260917",
            "LowerLimitPrice": 3000,
            "UpperLimitPrice": 3500,
        }
    ]
    with pytest.raises(CtpError) as error:
        client.create_order(CtpLimitOrderRequest(**BASE, price=3574.2))
    assert error.value.detail["code"] == "PRICE_OUT_OF_RANGE"
    assert error.value.detail["price_context"]["upper_bound"] == "3500"
    assert error.value.detail["order_identity"] is None
    assert client._session._order_ref == 10 and insert_count(api) == 0


@pytest.mark.parametrize(
    "change",
    [
        {"TradingDay": "20260916"},
        {"UpperLimitPrice": None},
        {"LowerLimitPrice": 5000, "UpperLimitPrice": 4000},
    ],
)
def test_stale_or_invalid_depth_never_sends(price_service, change):
    client, api = price_service
    api.queries["ReqQryDepthMarketData"] = [
        {
            "InstrumentID": "m2701",
            "ExchangeID": "DCE",
            "TradingDay": "20260917",
            "LowerLimitPrice": 3000,
            "UpperLimitPrice": 4000,
            **change,
        }
    ]
    with pytest.raises(CtpError) as error:
        client.create_order(CtpLimitOrderRequest(**BASE, price=3574.2))
    assert (
        error.value.detail["code"] == "PRICE_RULES_UNAVAILABLE"
        and insert_count(api) == 0
    )


def test_cache_reuses_only_static_info_and_reloads_after_login(price_service):
    client, api = price_service
    request = CtpLimitOrderRequest(**BASE, price=3574.2)
    client.create_order(request)
    client.create_order(request)
    methods = [m for m, _, _ in api.requests]
    assert (
        methods.count("ReqQryInstrument") == 1
        and methods.count("ReqQryDepthMarketData") == 2
    )
    api.callbacks.on_disconnected()
    api.callbacks.on_connected()
    client.create_order(request)
    methods = [m for m, _, _ in api.requests]
    assert (
        methods.count("ReqQryInstrument") == 2
        and methods.count("ReqQryDepthMarketData") == 3
    )


@pytest.mark.parametrize("query", ["ReqQryInstrument", "ReqQryDepthMarketData"])
def test_query_rejection_never_sends_order(price_service, query):
    client, api = price_service

    def reject(data, request_id):
        api.callbacks.on_response(
            query, {}, {"ErrorID": 99, "ErrorMsg": "查询被拒绝"}, request_id, True
        )
        return 0

    api.hooks[query] = reject
    with pytest.raises(CtpError) as error:
        client.create_order(CtpLimitOrderRequest(**BASE, price=3574.2))
    assert error.value.detail["code"] == "CTP_QUERY_FAILED"
    assert error.value.detail["order_identity"] is None and insert_count(api) == 0


def test_incomplete_query_times_out_before_order(price_service):
    client, api = price_service

    def incomplete(data, request_id):
        api.callbacks.on_response(
            "ReqQryInstrument",
            {"InstrumentID": "m2701", "ExchangeID": "DCE", "PriceTick": 1},
            {},
            request_id,
            False,
        )
        return 0

    api.hooks["ReqQryInstrument"] = incomplete
    with pytest.raises(CtpError) as error:
        client.create_order(CtpLimitOrderRequest(**BASE, price=3574.2))
    assert error.value.detail["code"] == "CTP_TIMEOUT" and insert_count(api) == 0


def test_market_order_does_not_require_price_queries(price_service):
    client, api = price_service
    result = client.create_order(CtpMarketOrderRequest(**BASE))
    assert result.price_adjustment is None
    assert result.order.OrderPriceType == "1"
    assert not any(method.startswith("ReqQry") for method, _, _ in api.requests)
