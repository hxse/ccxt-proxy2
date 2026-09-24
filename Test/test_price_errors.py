import json

import ccxt
import pytest

from src.tools.ccxt_errors import map_ccxt_exception


def test_binance_rejection_keeps_safe_code_and_dynamic_bound():
    raw = 'binance POST https://example/order?signature=private {"code":-4016,"msg":"Limit price can\'t be higher than 88368.95."}'
    result = map_ccxt_exception(ccxt.BadRequest(raw))
    assert result.status_code == 422 and result.code == "PRICE_OUT_OF_RANGE"
    assert result.detail["provider_code"] == -4016
    assert result.detail["price_context"]["upper_bound"] == "88368.95"
    assert "signature" not in json.dumps(result.detail) and "private" not in json.dumps(
        result.detail
    )


@pytest.mark.parametrize(
    "message",
    [
        "secret=private",
        "Limit price can't be higher than secret.",
        "<html>private</html>",
    ],
)
def test_arbitrary_upstream_message_is_not_exposed(message):
    exc = ccxt.BadRequest("binance " + json.dumps({"code": -4016, "msg": message}))
    result = map_ccxt_exception(exc)
    assert result.code == "PRICE_OUT_OF_RANGE"
    assert "price_context" not in result.detail
    assert "private" not in json.dumps(result.detail) and "secret" not in json.dumps(
        result.detail
    )


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("invalidPrice", "INVALID_ORDER_PRICE"),
        ("outsidePriceCollar", "PRICE_OUT_OF_RANGE"),
    ],
)
def test_real_kraken_sdk_exception_is_classified(code, expected):
    sdk = ccxt.krakenfutures()
    with pytest.raises(ccxt.InvalidOrder) as error:
        sdk.verify_order_action_success(code, "createOrder")
    result = map_ccxt_exception(error.value)
    assert result.code == expected and result.detail["provider_code"] == code


def test_kraken_spot_tick_error_is_safe():
    result = map_ccxt_exception(
        ccxt.InvalidOrder('kraken {"error":["EOrder:Tick size check failed"]}')
    )
    assert result.code == "INVALID_PRICE_PRECISION"
    assert result.detail["provider_code"] == "EOrder:Tick size check failed"


def test_unrecognized_error_keeps_existing_sanitized_mapping():
    result = map_ccxt_exception(
        ccxt.BadRequest('binance {"code":-1000,"msg":"private"}')
    )
    assert result.detail == {
        "code": "INVALID_PROVIDER_REQUEST",
        "message": "provider rejected the request parameters",
    }
