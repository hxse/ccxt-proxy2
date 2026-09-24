import math
from decimal import Decimal

import pytest

from src.order_prices import (
    OrderPriceError,
    PriceRules,
    PriceRulesUnavailable,
    prepare_price,
)


@pytest.mark.parametrize(("side", "expected"), [("buy", "3574"), ("sell", "3575")])
def test_normalization_preserves_user_price_bound(side, expected):
    result = prepare_price(3574.2, side, PriceRules(Decimal(1)))
    assert result.model_dump() == {
        "requested_price": "3574.2",
        "submitted_price": expected,
        "tick_size": "1",
        "adjusted": True,
    }


@pytest.mark.parametrize(
    ("price", "tick", "side", "expected"),
    [
        ("0.3", "0.1", "buy", "0.3"),
        ("0.30000000000000004", "0.1", "buy", "0.3"),
        ("0.30000000000000004", "0.1", "sell", "0.4"),
        ("1.2499999999999999999999999999999999", "0.25", "buy", "1"),
        ("1.2500000000000000000000000000000001", "0.25", "sell", "1.5"),
    ],
)
def test_decimal_grid_has_no_binary_rounding(price, tick, side, expected):
    assert (
        prepare_price(price, side, PriceRules(Decimal(tick))).submitted_price
        == expected
    )


@pytest.mark.parametrize(
    ("value", "side", "expected"),
    [
        (0.1 + 0.2, "sell", "0.3"),
        (math.nextafter(0.3, -math.inf), "buy", "0.3"),
        (100.10000000000001, "sell", "100.1"),
        (math.nextafter(100.1, -math.inf), "buy", "100.1"),
    ],
)
def test_float_noise_restores_nearest_grid_and_keeps_original(value, side, expected):
    result = prepare_price(value, side, PriceRules(Decimal("0.1")))
    assert result.submitted_price == expected
    assert result.requested_price == str(value)
    assert result.adjusted is True


@pytest.mark.parametrize(("steps", "expected"), [(2, "0.5"), (3, "0.6")])
def test_float_noise_tolerance_does_not_exceed_two_ulps(steps, expected):
    value = 0.5
    for _ in range(steps):
        value = math.nextafter(value, math.inf)
    assert (
        prepare_price(value, "sell", PriceRules(Decimal("0.1"))).submitted_price
        == expected
    )


def test_float_noise_tolerance_is_also_capped_by_tick_size():
    result = prepare_price(
        100.10000000000001, "sell", PriceRules(Decimal("0.000000001"))
    )
    assert result.submitted_price == "100.100000001"


@pytest.mark.parametrize(
    ("value", "side", "expected"),
    [
        (100.100000001, "sell", "100.2"),
        (100.099999999, "buy", "100"),
        (Decimal("100.10000000000001"), "sell", "100.2"),
    ],
)
def test_meaningful_offsets_and_exact_decimal_prices_are_not_swallowed(
    value, side, expected
):
    assert (
        prepare_price(value, side, PriceRules(Decimal("0.1"))).submitted_price
        == expected
    )


def test_float_noise_uses_nonzero_grid_origin():
    rules = PriceRules(Decimal("0.5"), Decimal("0.1"), Decimal("10.1"), Decimal("0.1"))
    assert (
        prepare_price(math.nextafter(1.1, math.inf), "sell", rules).submitted_price
        == "1.1"
    )


def test_float_noise_does_not_relax_price_bounds():
    rules = PriceRules(Decimal("0.1"), upper=Decimal("100.09999999999999"))
    with pytest.raises(OrderPriceError) as error:
        prepare_price(100.10000000000001, "sell", rules)
    assert error.value.code == "PRICE_OUT_OF_RANGE"
    assert error.value.detail["price_context"]["submitted_price"] == "100.1"
    assert error.value.detail["price_context"]["upper_bound"] == "100.09999999999999"


def test_smallest_positive_float_is_not_snapped_to_zero_for_sell():
    assert (
        prepare_price(math.ulp(0.0), "sell", PriceRules(Decimal("0.1"))).submitted_price
        == "0.1"
    )


@pytest.mark.parametrize("value", [0.1 + 0.2, 100.10000000000001])
def test_trigger_price_still_rejects_float_noise(value):
    with pytest.raises(OrderPriceError) as error:
        prepare_price(
            value,
            "sell",
            PriceRules(Decimal("0.1")),
            strict=True,
            field="stopLossPrice",
        )
    assert error.value.code == "INVALID_PRICE_PRECISION"


def test_nonzero_grid_origin():
    rules = PriceRules(Decimal("0.5"), Decimal("0.1"), Decimal("10.1"), Decimal("0.1"))
    assert prepare_price("1.5", "buy", rules).submitted_price == "1.1"
    assert prepare_price("1.5", "sell", rules).submitted_price == "1.6"


@pytest.mark.parametrize("price", [0, -1, float("nan"), float("inf"), True, "bad"])
def test_invalid_price_is_a_pre_submission_error(price):
    with pytest.raises(OrderPriceError) as error:
        prepare_price(price, "buy", PriceRules(Decimal(1)))
    assert error.value.code == "INVALID_ORDER_PRICE"


def test_rounding_to_zero_is_rejected():
    with pytest.raises(OrderPriceError) as error:
        prepare_price("0.1", "buy", PriceRules(Decimal(1)))
    assert error.value.code == "INVALID_ORDER_PRICE"
    assert error.value.detail["price_context"]["submitted_price"] == "0"


def test_bounds_check_uses_submitted_price_and_does_not_clamp():
    rules = PriceRules(Decimal(1), Decimal(10), Decimal(20))
    assert prepare_price("20.2", "buy", rules).submitted_price == "20"
    with pytest.raises(OrderPriceError) as error:
        prepare_price("20.2", "sell", rules)
    assert error.value.code == "PRICE_OUT_OF_RANGE"
    assert error.value.detail["price_context"]["submitted_price"] == "21"
    assert error.value.detail["price_context"]["upper_bound"] == "20"


def test_trigger_threshold_is_not_moved():
    with pytest.raises(OrderPriceError) as error:
        prepare_price(
            "3574.2", "sell", PriceRules(Decimal(1)), field="stopLossPrice", strict=True
        )
    assert error.value.code == "INVALID_PRICE_PRECISION"
    assert error.value.detail["price_context"]["field"] == "stopLossPrice"


@pytest.mark.parametrize("tick", ["0", "-1", "NaN", "Infinity"])
def test_invalid_rules_fail_closed(tick):
    with pytest.raises(PriceRulesUnavailable):
        PriceRules(Decimal(tick))


def test_inverted_bounds_are_unavailable():
    with pytest.raises(PriceRulesUnavailable):
        PriceRules(Decimal(1), Decimal(20), Decimal(10))
