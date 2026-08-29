import pytest
from fastapi.testclient import TestClient

EXCHANGE = "binance"
MARKET = "future"
MODE = "sandbox"
SYMBOL = "BTC/USDT:USDT"


@pytest.fixture
def tracked_order_ids(client: TestClient):
    order_ids: list[str] = []
    yield order_ids
    for order_id in reversed(order_ids):
        response = client.post(
            "/ccxt/cancel_order",
            json={
                "exchange_name": EXCHANGE,
                "market": MARKET,
                "mode": MODE,
                "symbol": SYMBOL,
                "id": order_id,
            },
        )
        if response.status_code not in (200, 404):
            print(f"Cleanup failed for order {order_id}: {response.text}")


def test_order_lifecycle(client: TestClient, tracked_order_ids: list[str]):
    ticker_response = client.get(
        "/ccxt/fetch_tickers",
        params={
            "exchange_name": EXCHANGE,
            "market": MARKET,
            "mode": MODE,
            "symbols": SYMBOL,
        },
    )
    assert ticker_response.status_code == 200
    last_price = ticker_response.json()["tickers"][SYMBOL]["last"]
    assert isinstance(last_price, (int, float)) and last_price > 0

    # 1. Create Limit
    limit_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "side": "buy",
        "amount": 0.005,
        "price": round(last_price * 0.8, 1),
    }
    res_l = client.post("/ccxt/create_limit_order", json=limit_payload)
    assert res_l.status_code == 200
    order_l = res_l.json().get("order", {})
    assert order_l["symbol"] == SYMBOL
    assert order_l["side"] == "buy"
    assert order_l["type"] == "limit"
    assert order_l["status"] in ["open", "closed", "new"]

    order_id = order_l.get("id")
    order_ts = order_l.get("timestamp")
    assert order_id
    tracked_order_ids.append(order_id)

    # 2. Create Stop Market
    stop_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "side": "sell",
        "amount": 0.005,
        "triggerPrice": round(last_price * 0.5, 1),
        "reduceOnly": False,
    }
    res_s = client.post("/ccxt/create_stop_market_order", json=stop_payload)
    assert res_s.status_code == 200
    order_s = res_s.json().get("order", {})
    assert order_s["side"] == "sell"
    stop_order_id = order_s.get("id")
    assert stop_order_id
    tracked_order_ids.append(stop_order_id)

    # 3. Fetch Open Orders
    fetch_params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
    }
    res_o = client.get("/ccxt/fetch_open_orders", params=fetch_params)
    assert res_o.status_code == 200
    orders = res_o.json().get("orders", [])
    open_ids = {order["id"] for order in orders}
    assert {order_id, stop_order_id} <= open_ids

    # 4. Fetch Single
    fetch_single_params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "id": order_id,
    }
    res_f = client.get("/ccxt/fetch_order", params=fetch_single_params)
    assert res_f.status_code == 200
    fetched_order = res_f.json().get("order", {})
    assert fetched_order["id"] == order_id

    # 5. Cancel Single
    cancel_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "id": order_id,
    }
    res_c = client.post("/ccxt/cancel_order", json=cancel_payload)
    assert res_c.status_code == 200

    # 6. Cancel the stop created by this test; never touch unrelated sandbox orders.
    stop_cancel_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "id": stop_order_id,
    }
    res_stop_cancel = client.post("/ccxt/cancel_order", json=stop_cancel_payload)
    assert res_stop_cancel.status_code == 200

    # 7. Fetch Closed (Debug & Since)
    # verify status first
    print("\n[DEBUG] Verifying single order status via direct fetch...")
    res_verify = client.get("/ccxt/fetch_order", params=fetch_single_params)
    assert res_verify.status_code == 200
    v_order = res_verify.json().get("order", {})
    assert v_order.get("status") in {"canceled", "closed"}

    # Use explicit since
    fetch_params_since = fetch_params.copy()
    if order_ts is not None:
        fetch_params_since["since"] = str(order_ts - 60_000)

    res_cl = client.get("/ccxt/fetch_closed_orders", params=fetch_params_since)
    assert res_cl.status_code == 200
    assert isinstance(res_cl.json().get("orders", []), list)

    # 8. Fetch My Trades
    res_tr = client.get("/ccxt/fetch_my_trades", params=fetch_params)
    assert res_tr.status_code == 200
