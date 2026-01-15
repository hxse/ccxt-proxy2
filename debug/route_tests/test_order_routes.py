from fastapi.testclient import TestClient
import time
import pytest

EXCHANGE = "binance"
MARKET = "future"
MODE = "sandbox"
SYMBOL = "BTC/USDT:USDT"


def test_order_lifecycle(client: TestClient):
    # 0. Clean (Cancel All)
    cleanup_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
    }
    response = client.post("/ccxt/cancel_all_orders", json=cleanup_payload)
    assert response.status_code == 200

    # 1. Create Limit
    limit_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "side": "buy",
        "amount": 0.005,
        "price": 50000.0,  # safely low
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

    # 2. Create Stop Market
    stop_payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "side": "sell",
        "amount": 0.005,
        "triggerPrice": 40000.0,
        "reduceOnly": False,
    }
    res_s = client.post("/ccxt/create_stop_market_order", json=stop_payload)
    assert res_s.status_code == 200
    order_s = res_s.json().get("order", {})
    assert order_s["side"] == "sell"

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
    assert len(orders) >= 2

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

    # 6. Cancel All
    res_ca = client.post("/ccxt/cancel_all_orders", json=cleanup_payload)
    assert res_ca.status_code == 200

    # 7. Fetch Closed (Debug & Since)
    # verify status first
    print("\n[DEBUG] Verifying single order status via direct fetch...")
    res_verify = client.get("/ccxt/fetch_order", params=fetch_single_params)
    if res_verify.status_code == 200:
        v_order = res_verify.json().get("order", {})
        print(
            f"[DEBUG] Single Order Status: {v_order.get('status')} | ID: {v_order.get('id')} | TS: {v_order.get('timestamp')}"
        )
    else:
        print(f"[DEBUG] Single Order Fetch Failed: {res_verify.status_code}")

    # Use explicit since
    since_ts = order_ts - 60000 if order_ts else int((time.time() - 3600) * 1000)
    fetch_params_since = fetch_params.copy()
    fetch_params_since["since"] = since_ts

    found_closed = False
    closed_orders = []

    for attempt in range(3):
        print(f"Fetch Closed Attempt {attempt + 1} (since={since_ts})...")
        time.sleep(10)  # Increased sleep to handle Sandbox latency
        res_cl = client.get("/ccxt/fetch_closed_orders", params=fetch_params_since)
        assert res_cl.status_code == 200
        closed_orders = res_cl.json().get("orders", [])

        if any(o["id"] == order_id for o in closed_orders):
            found_closed = True
            break

    if not found_closed:
        print(f"\n[DEBUG] Target Order ID: {order_id}")
        print(f"[DEBUG] Fetched Count: {len(closed_orders)}")
        if closed_orders:
            print(f"[DEBUG] Top 5 Recent IDs: {[o['id'] for o in closed_orders[:5]]}")
            print(
                f"[DEBUG] Top 5 Timestamps: {[o.get('timestamp') for o in closed_orders[:5]]}"
            )
        else:
            print("[DEBUG] No orders returned.")

    found_closed = any(o["id"] == order_id for o in closed_orders)
    assert found_closed, (
        f"Canceled order {order_id} not found in closed orders history."
    )

    # 8. Fetch My Trades
    res_tr = client.get("/ccxt/fetch_my_trades", params=fetch_params)
    assert res_tr.status_code == 200
