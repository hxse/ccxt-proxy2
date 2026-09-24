import copy

import ccxt
import pytest

from src.domain_errors import NetworkIncomplete
from src.order_prices import OrderPriceError, PriceRulesUnavailable
from src.tools.ccxt_client import CcxtClient
from Test.test_ccxt_client import FakeExchange

SYMBOL = "BTC/USDT:USDT"


def price_client(provider="binance", kind="future"):
    exchange = FakeExchange([])
    market = copy.deepcopy(exchange.market(SYMBOL))
    market["limits"]["price"] = {"min": 0.1, "max": 1000000}
    exchange.market = lambda symbol: market
    return CcxtClient(exchange, provider, kind, "sandbox", None), exchange, market


@pytest.mark.parametrize(
    ("side", "expected"), [("buy", "50000.1"), ("sell", "50000.2")]
)
def test_client_submits_directionally_aligned_price(side, expected):
    client, exchange, _ = price_client()
    result = client.create_order(SYMBOL, "limit", side, 1, 50000.15)
    assert exchange.create_arguments[0][0][4] == expected
    assert result["price_adjustment"] == {
        "requested_price": "50000.15",
        "submitted_price": expected,
        "tick_size": "0.1",
        "adjusted": True,
    }


@pytest.mark.parametrize(
    ("side", "price", "bound", "expected"),
    [
        ("buy", 52500.2, "upper_bound", "52500.00"),
        ("sell", 47499.8, "lower_bound", "47500.00"),
    ],
)
def test_binance_futures_dynamic_rejection_precedes_write(side, price, bound, expected):
    client, exchange, _ = price_client()
    with pytest.raises(OrderPriceError) as error:
        client.create_order(SYMBOL, "limit", side, 1, price)
    assert error.value.code == "PRICE_OUT_OF_RANGE"
    assert float(error.value.detail["price_context"][bound]) == float(expected)
    assert exchange.create_calls == 0


@pytest.mark.parametrize(
    ("side", "price"),
    [("buy", 40000), ("sell", 60000), ("buy", 52500), ("sell", 47500)],
)
def test_binance_futures_only_checks_relevant_side_and_includes_boundary(side, price):
    client, exchange, _ = price_client()
    client.create_order(SYMBOL, "limit", side, 1, price)
    assert exchange.create_calls == 1


def test_static_limit_checked_without_reading_reference_quote():
    client, exchange, _ = price_client()
    exchange.fapiPublicGetPremiumIndex = lambda params: pytest.fail("不应查询行情")
    with pytest.raises(OrderPriceError) as error:
        client.create_order(SYMBOL, "limit", "buy", 1, 1000001)
    assert error.value.code == "PRICE_OUT_OF_RANGE" and exchange.create_calls == 0


def test_sdk_cannot_silently_change_normalized_price():
    client, exchange, market = price_client()
    market["precision"]["price"] = 1
    with pytest.raises(PriceRulesUnavailable, match="SDK 精度规则"):
        client.create_order(SYMBOL, "limit", "buy", 1, 50000.15)
    assert exchange.create_calls == 0


def test_reference_for_another_symbol_is_not_used():
    client, exchange, _ = price_client()
    exchange.fapiPublicGetPremiumIndex = lambda params: {
        "symbol": "ETHUSDT",
        "markPrice": "50000",
    }
    with pytest.raises(PriceRulesUnavailable):
        client.create_order(SYMBOL, "limit", "buy", 1, 50000)
    assert exchange.create_calls == 0


@pytest.mark.parametrize("reference", [None, "NaN", 0, -1])
def test_missing_mark_price_never_sends_order(reference):
    client, exchange, _ = price_client()
    exchange.fapiPublicGetPremiumIndex = lambda params: {"markPrice": reference}
    with pytest.raises(PriceRulesUnavailable):
        client.create_order(SYMBOL, "limit", "buy", 1, 50000)
    assert exchange.create_calls == 0


def test_read_failure_never_becomes_unknown_write_status():
    client, exchange, _ = price_client()

    def fail(params):
        raise NetworkIncomplete("价格行情读取失败")

    exchange.fapiPublicGetPremiumIndex = fail
    with pytest.raises(NetworkIncomplete):
        client.create_order(SYMBOL, "limit", "buy", 1, 50000)
    assert exchange.create_calls == 0


@pytest.mark.parametrize(
    "extra",
    [
        {"limitPrice": 1},
        {"priceMatch": "OPPONENT"},
        {"pair": "ETHUSD"},
        {"orderType": "mkt"},
    ],
)
def test_provider_extensions_cannot_replace_validated_price(extra):
    client, exchange, _ = price_client()
    with pytest.raises(OrderPriceError) as error:
        client.create_order(SYMBOL, "limit", "buy", 1, 50000, params=extra)
    assert error.value.code == "INVALID_ORDER_PRICE" and exchange.create_calls == 0


def test_trigger_uses_static_grid_without_current_market_collar():
    client, exchange, _ = price_client()
    exchange.fapiPublicGetPremiumIndex = lambda params: pytest.fail(
        "触发价不查当前价格边界"
    )
    client.create_order(SYMBOL, "market", "sell", 1, params={"takeProfitPrice": 70000})
    with pytest.raises(OrderPriceError) as error:
        client.create_order(
            SYMBOL, "market", "sell", 1, params={"stopLossPrice": 40000.15}
        )
    assert error.value.code == "INVALID_PRICE_PRECISION" and exchange.create_calls == 1


def test_binance_spot_uses_side_specific_average_window():
    client, exchange, market = price_client(kind="spot")
    market["info"]["filters"][1] = {
        "filterType": "PERCENT_PRICE_BY_SIDE",
        "avgPriceMins": 5,
        "bidMultiplierDown": "0.9",
        "bidMultiplierUp": "1.1",
        "askMultiplierDown": "0.8",
        "askMultiplierUp": "1.2",
    }
    exchange.publicGetAvgPrice = lambda params: {"mins": 5, "price": "100"}
    with pytest.raises(OrderPriceError):
        client.create_order(SYMBOL, "limit", "buy", 1, 115)
    client.create_order(SYMBOL, "limit", "sell", 1, 115)
    assert exchange.create_calls == 1
    exchange.publicGetAvgPrice = lambda params: {"mins": 1, "price": "100"}
    with pytest.raises(PriceRulesUnavailable):
        client.create_order(SYMBOL, "limit", "buy", 1, 100)
    assert exchange.create_calls == 1


def test_binance_spot_zero_window_uses_latest_trade():
    client, exchange, market = price_client(kind="spot")
    market["info"]["filters"][1]["avgPriceMins"] = 0
    exchange.publicGetTickerPrice = lambda params: {"symbol": "BTCUSDT", "price": "100"}
    client.create_order(SYMBOL, "limit", "buy", 1, 100)
    with pytest.raises(OrderPriceError):
        client.create_order(SYMBOL, "limit", "buy", 1, 94)
    assert exchange.create_calls == 1


@pytest.mark.parametrize(
    ("side", "price", "accepted"),
    [
        ("buy", 60000, True),
        ("buy", 60000.1, False),
        ("sell", 40000, True),
        ("sell", 39999.9, False),
        ("buy", 30000, True),
        ("sell", 70000, True),
    ],
)
def test_kraken_collar_applies_only_when_crossing_spread(side, price, accepted):
    client, exchange, _ = price_client("kraken")
    if accepted:
        client.create_order(SYMBOL, "limit", side, 1, price)
    else:
        with pytest.raises(OrderPriceError) as error:
            client.create_order(SYMBOL, "limit", side, 1, price)
        assert error.value.code == "PRICE_OUT_OF_RANGE"
    assert exchange.create_calls == int(accepted)


def test_kraken_spot_decimal_precision_and_no_futures_collar():
    client, exchange, market = price_client("kraken", "spot")
    exchange.precisionMode = ccxt.DECIMAL_PLACES
    market["precision"]["price"] = 2
    exchange.fetch_ticker = lambda symbol: pytest.fail("现货不套合约保护规则")
    client.create_order(SYMBOL, "limit", "sell", 1, 123.451)
    assert exchange.create_arguments[0][0][4] == "123.46"


@pytest.mark.parametrize("provider", ["binance", "kraken"])
def test_actual_sdk_builder_keeps_backend_price(provider):
    sdk = ccxt.binance() if provider == "binance" else ccxt.krakenfutures()
    symbol = SYMBOL if provider == "binance" else "BTC/USD:USD"
    market = {
        "id": "BTCUSDT" if provider == "binance" else "PF_XBTUSD",
        "symbol": symbol,
        "base": "BTC",
        "quote": "USDT" if provider == "binance" else "USD",
        "settle": "USD",
        "type": "swap",
        "spot": False,
        "swap": True,
        "future": False,
        "contract": True,
        "linear": True,
        "inverse": False,
        "contractSize": 1,
        "precision": {"amount": 0.001, "price": 0.1},
        "info": {
            "orderTypes": ["LIMIT"],
            "tickSize": 0.1,
            "filters": [
                {
                    "filterType": "PRICE_FILTER",
                    "tickSize": "0.1",
                    "minPrice": "0.1",
                    "maxPrice": "1000000",
                },
                {
                    "filterType": "PERCENT_PRICE",
                    "multiplierUp": "1.05",
                    "multiplierDown": "0.95",
                },
            ],
        },
    }
    sdk.set_markets([market])
    sdk.fetch = lambda *a, **kw: pytest.fail("禁止网络")
    sdk.fapiPublicGetPremiumIndex = lambda params: {"markPrice": "50000"}
    sdk.fetch_ticker = lambda symbol: {"markPrice": 50000, "bid": 49999, "ask": 50001}
    captured = []

    def create(*args, **kwargs):
        captured.append(sdk.create_order_request(*args, **kwargs))
        return {"id": "offline"}

    sdk.create_order = create
    client = CcxtClient(sdk, provider, "future", "sandbox", None)
    result = client.create_order(symbol, "limit", "sell", 0.01, 50000.15)
    assert captured[0]["price" if provider == "binance" else "limitPrice"] == "50000.2"
    assert result["price_adjustment"]["submitted_price"] == "50000.2"
