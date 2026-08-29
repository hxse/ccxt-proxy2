import pytest

from src.domain_errors import InvalidProviderData, NetworkIncomplete
from src.responses import MarketInfoResponse
from Test.test_ccxt_client import _client


def test_binance_futures_order_translation_lives_on_client(temp_dir):
    client, _ = _client(temp_dir)

    orders = client.fetch_open_orders("BTC/USDT:USDT", None, None)
    fetched = client.fetch_order("stop", "BTC/USDT:USDT")
    canceled = client.cancel_all_orders("BTC/USDT:USDT")

    assert [order["id"] for order in orders] == ["stop", "normal"]
    assert fetched["id"] == "stop"
    assert canceled == [[{"stop": False}], [{"stop": True}]]


def test_trigger_order_translation_lives_on_client(temp_dir):
    client, exchange = _client(temp_dir)

    client.create_stop_market_order(
        "BTC/USDT:USDT",
        "sell",
        0.1,
        50_000,
        reduce_only=True,
        client_order_id="stop-1",
        time_in_force="GTC",
        params={"workingType": "MARK_PRICE"},
    )

    assert exchange.create_arguments[-1][1]["params"] == {
        "workingType": "MARK_PRICE",
        "reduceOnly": True,
        "stopLossPrice": 50_000,
        "clientOrderId": "stop-1",
        "timeInForce": "GTC",
    }


def test_take_profit_translation_uses_distinct_trigger_field(temp_dir):
    client, exchange = _client(temp_dir)

    client.create_take_profit_market_order(
        "BTC/USDT:USDT",
        "sell",
        0.1,
        60_000,
        reduce_only=True,
        client_order_id=None,
        time_in_force=None,
        params=None,
    )

    assert exchange.create_arguments[-1][1]["params"] == {
        "reduceOnly": True,
        "takeProfitPrice": 60_000,
    }


def test_binance_spot_does_not_apply_futures_order_split(temp_dir):
    client, _ = _client(temp_dir, provider="binance", market="spot")

    orders = client.fetch_open_orders("BTC/USDT", None, None)

    assert [order["id"] for order in orders] == ["normal"]


def test_kraken_future_does_not_apply_binance_order_split(temp_dir):
    client, _ = _client(temp_dir, provider="kraken", market="future")

    orders = client.fetch_open_orders("BTC/USD:USD", None, None)

    assert [order["id"] for order in orders] == ["normal"]


def test_binance_order_split_deduplicates_same_id_with_stop_result(temp_dir):
    client, exchange = _client(temp_dir)

    def duplicate_order(symbol, since, limit, params):
        return [
            {
                "id": "same",
                "timestamp": 1,
                "source": "stop" if params.get("stop") else "ordinary",
            }
        ]

    exchange.fetch_open_orders = duplicate_order

    orders = client.fetch_open_orders("BTC/USDT:USDT", None, None)

    assert orders == [{"id": "same", "timestamp": 1, "source": "stop"}]


def test_binance_order_split_applies_limit_after_merge(temp_dir):
    client, _ = _client(temp_dir)

    orders = client.fetch_open_orders("BTC/USDT:USDT", None, 1)

    assert [order["id"] for order in orders] == ["stop"]


def test_binance_cancel_order_falls_back_to_conditional_endpoint(temp_dir):
    client, exchange = _client(temp_dir)

    canceled = client.cancel_order("stop", "BTC/USDT:USDT", params={"stop": False})
    canceled_all = client.cancel_all_orders("BTC/USDT:USDT", params={"stop": False})

    assert canceled == {"id": "stop", "status": "canceled"}
    assert canceled_all == [[{"stop": False}], [{"stop": True}]]
    assert exchange.create_calls == 0


def test_close_position_filters_side_and_uses_reduce_only_order(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.positions = [
        {"symbol": "BTC/USDT:USDT", "side": "long", "contracts": 2},
        {"symbol": "BTC/USDT:USDT", "side": "short", "contracts": 3},
    ]

    remaining = client.close_position(
        "BTC/USDT:USDT",
        side="long",
        params={"workingType": "MARK_PRICE", "reduceOnly": False},
    )

    assert remaining == exchange.positions
    assert len(exchange.create_arguments) == 1
    args, kwargs = exchange.create_arguments[0]
    assert args[:4] == ("BTC/USDT:USDT", "market", "sell", 2.0)
    assert kwargs["params"] == {
        "reduceOnly": True,
        "workingType": "MARK_PRICE",
    }


def test_close_position_rejects_nonzero_position_without_valid_side(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.positions = [{"symbol": "BTC/USDT:USDT", "side": None, "contracts": 2}]

    with pytest.raises(InvalidProviderData, match="without a valid side"):
        client.close_position("BTC/USDT:USDT")

    assert exchange.create_arguments == []


def test_market_info_uses_nullable_leverage_when_no_position_exists(temp_dir):
    client, _ = _client(temp_dir)

    result = client.fetch_market_info("BTC/USDT:USDT")

    assert result["leverage"] is None
    assert MarketInfoResponse.model_validate(result).leverage is None


def test_market_info_returns_provider_position_leverage(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.positions = [{"symbol": "BTC/USDT:USDT", "leverage": "20"}]

    result = client.fetch_market_info("BTC/USDT:USDT")

    assert result["leverage"] == 20


def test_market_info_does_not_hide_position_failure(temp_dir):
    client, exchange = _client(temp_dir)

    def fail(*args, **kwargs):
        raise NetworkIncomplete("positions unavailable")

    exchange.fetch_positions = fail

    with pytest.raises(NetworkIncomplete, match="positions unavailable"):
        client.fetch_market_info("BTC/USDT:USDT")


def test_market_info_rejects_invalid_provider_leverage(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.positions = [{"symbol": "BTC/USDT:USDT", "leverage": "unknown"}]

    with pytest.raises(InvalidProviderData, match="invalid leverage"):
        client.fetch_market_info("BTC/USDT:USDT")
