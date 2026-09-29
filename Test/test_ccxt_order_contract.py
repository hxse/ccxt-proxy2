"""隔离 HTTP 到真实 SDK 请求/回执的回归；所有网络在 fetch 边界被替换。"""

import ccxt
import pytest

from src.responses import OrderResponse, OrdersResponse
from Test.helpers.ccxt_order_sdk import (
    ORDER_ID,
    STAMP,
    kraken_send_reply,
    route_client,
    sdk_client,
)


def limit_body(symbol, **extra):
    return {
        "exchange_name": "kraken",
        "market": "future",
        "is_live": True,
        "symbol": symbol,
        "side": "buy",
        "amount": 1,
        "price": 50000,
        **extra,
    }


@pytest.mark.parametrize("tif,native", [("IOC", "ioc"), ("FOK", "fok")])
@pytest.mark.parametrize("side", ["buy", "sell"])
def test_kraken_immediate_limit_reaches_native_http_without_gtc(
    monkeypatch, tif, native, side
):
    client, calls, symbol = sdk_client(monkeypatch, "kraken", kraken_send_reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/create_limit_order",
            json=limit_body(symbol, timeInForce=tif, side=side, amount="1"),
        )
        assert response.status_code == 200
        assert len(calls) == 1
        sent = calls[0][2]
        assert sent["orderType"] == native
        assert sent["limitPrice"] == "50000" and sent["size"] == "1"
        assert sent["side"] == side and "timeInForce" not in sent
        order = response.json()["order"]
        assert order["id"] == ORDER_ID and order["timeInForce"] == native
        assert order["status"] == "closed"
        assert order["info"]["orderEvents"][0]["orderPriorExecution"]["type"] == native
    finally:
        client.close()


@pytest.mark.parametrize(
    "extra,native",
    [
        ({}, "lmt"),
        ({"timeInForce": "GTC"}, "lmt"),
        ({"timeInForce": "GTC", "postOnly": True}, "post"),
    ],
)
def test_kraken_default_gtc_and_post_only_keep_native_behavior(
    monkeypatch, extra, native
):
    client, calls, symbol = sdk_client(monkeypatch, "kraken", kraken_send_reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/create_limit_order", json=limit_body(symbol, **extra)
        )
        assert response.status_code == 200
        assert calls[0][2]["orderType"] == native
        assert response.json()["order"]["timeInForce"] == "gtc"
    finally:
        client.close()


@pytest.mark.parametrize(
    "extra",
    [
        {"postOnly": True},
        {"orderType": "lmt"},
        {"stopLossPrice": 49000},
        {"takeProfitPrice": 51000},
    ],
)
@pytest.mark.parametrize("tif", ["IOC", "FOK"])
def test_kraken_conflicting_immediate_flags_fail_before_write(monkeypatch, extra, tif):
    client, calls, symbol = sdk_client(monkeypatch, "kraken", kraken_send_reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/create_limit_order",
            json=limit_body(symbol, timeInForce=tif, **extra),
        )
        assert response.status_code == 422
        assert response.json()["detail"]["code"] == "INVALID_PROVIDER_REQUEST"
        assert calls == []
    finally:
        client.close()


def test_direct_client_params_are_copied_and_lowercase_is_supported(monkeypatch):
    client, calls, symbol = sdk_client(monkeypatch, "kraken", kraken_send_reply)
    params = {
        "timeInForce": "fok",
        "orderType": "fok",
        "clientOrderId": "offline-client",
    }
    original = dict(params)
    try:
        result = client.create_order(symbol, "limit", "buy", 1, 50000, params)
        assert calls[0][2]["orderType"] == "fok"
        assert calls[0][2]["cliOrdId"] == "offline-client"
        assert params == original
        assert result["timeInForce"] == "fok"
    finally:
        client.close()


def test_real_kraken_status_parser_can_return_unknown_order_type(monkeypatch):
    # orders/status 的 order.type=ORDER 不是 limit/market；SDK 正确保留 type=null。
    raw = {
        "order": {
            "type": "ORDER",
            "orderId": ORDER_ID,
            "cliOrdId": None,
            "symbol": "PF_XBTUSD",
            "side": "buy",
            "quantity": "1",
            "filled": "0",
            "limitPrice": "50000",
            "reduceOnly": False,
            "timestamp": STAMP,
            "lastUpdateTimestamp": STAMP,
        },
        "status": "ENTERED_BOOK",
        "updateReason": None,
        "error": None,
    }

    def reply(method, path, params):
        assert path.endswith("/orders/status")
        return {"result": "success", "serverTime": STAMP, "orders": [raw]}

    client, calls, symbol = sdk_client(monkeypatch, "kraken", reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.get(
            "/ccxt/fetch_order",
            params={
                "exchange_name": "kraken",
                "market": "future",
                "is_live": True,
                "symbol": symbol,
                "id": ORDER_ID,
            },
        )
        assert response.status_code == 200 and len(calls) == 1
        order = response.json()["order"]
        assert order["id"] == ORDER_ID and order["type"] is None
        assert order["side"] == "buy" and order["info"] == raw
    finally:
        client.close()


def test_real_binance_conditional_cancel_preserves_success_receipt(monkeypatch):
    # 官方 Cancel Algo Order 的成功形状只有标识和结果，无 type/side。
    raw = {
        "algoId": 2146760,
        "clientAlgoId": "offline-conditional",
        "code": "200",
        "msg": "success",
    }

    def reply(method, path, params):
        assert method == "DELETE"
        if path.endswith("/algoOrder"):
            assert params["algoId"] == "2146760"
            return raw
        assert path.endswith("/order")
        raise ccxt.OrderNotFound('binance {"code":-2011,"msg":"Unknown order sent."}')

    client, calls, symbol = sdk_client(monkeypatch, "binance", reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/cancel_order",
            json={
                "exchange_name": "binance",
                "market": "future",
                "is_live": True,
                "symbol": symbol,
                "id": "2146760",
            },
        )
        assert response.status_code == 200
        assert [call[1] for call in calls] == ["/fapi/v1/order", "/fapi/v1/algoOrder"]
        order = response.json()["order"]
        assert order["id"] == "2146760" and order["info"] == raw
        assert (
            order["type"] is None and order["side"] is None and order["status"] is None
        )
    finally:
        client.close()


def test_cancel_timeout_is_unknown_without_fallback_or_retry(monkeypatch):
    def reply(*args):
        raise ccxt.RequestTimeout("offline network timeout")

    client, calls, symbol = sdk_client(monkeypatch, "binance", reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/cancel_order",
            json={
                "exchange_name": "binance",
                "market": "future",
                "is_live": True,
                "symbol": symbol,
                "id": "2146760",
            },
        )
        assert response.status_code == 502
        assert response.json()["detail"]["code"] == "OPERATION_STATUS_UNKNOWN"
        assert len(calls) == 1
    finally:
        client.close()


def test_unknown_fields_work_in_lists_and_are_nullable_in_schema():
    payload = {
        "id": "known",
        "symbol": "BTC/USDT:USDT",
        "status": None,
        "info": {"code": "200"},
    }
    single = OrderResponse.model_validate({"order": payload}).model_dump()["order"]
    listed = OrdersResponse.model_validate({"orders": [payload]}).model_dump()[
        "orders"
    ][0]
    assert single == listed and single["type"] is None and single["side"] is None
    properties = OrderResponse.model_json_schema()["$defs"]["OrderStructure"][
        "properties"
    ]
    for key in ("type", "side"):
        assert {value["type"] for value in properties[key]["anyOf"]} == {
            "string",
            "null",
        }


def test_binance_ioc_keeps_unified_encoding(monkeypatch):
    def reply(method, path, params):
        if path.endswith("/premiumIndex"):
            assert method == "GET"
            return {"symbol": "BTCUSDT", "markPrice": "50000"}
        assert method == "POST" and path == "/fapi/v1/order"
        assert params["type"] == "LIMIT" and params["timeInForce"] == "IOC"
        assert "orderType" not in params
        return {
            "orderId": 123,
            "symbol": "BTCUSDT",
            "side": params["side"],
            "type": "LIMIT",
            "timeInForce": "IOC",
            "status": "EXPIRED",
            "price": params["price"],
            "origQty": params["quantity"],
            "executedQty": "0",
        }

    client, calls, symbol = sdk_client(monkeypatch, "binance", reply)
    http, _ = route_client(monkeypatch, client)
    try:
        response = http.post(
            "/ccxt/create_limit_order",
            json=limit_body(symbol, exchange_name="binance", timeInForce="IOC"),
        )
        assert response.status_code == 200
        assert response.json()["order"]["timeInForce"] == "IOC"
        assert len(calls) == 2
    finally:
        client.close()
