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


def test_binance_spot_does_not_apply_futures_order_split(temp_dir):
    client, _ = _client(temp_dir, provider="binance", market="spot")

    orders = client.fetch_open_orders("BTC/USDT", None, None)

    assert [order["id"] for order in orders] == ["normal"]
