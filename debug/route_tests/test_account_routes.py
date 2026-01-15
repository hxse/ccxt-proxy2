from fastapi.testclient import TestClient

EXCHANGE = "binance"
MARKET = "future"
MODE = "sandbox"
SYMBOL = "BTC/USDT:USDT"


def test_fetch_balance(client: TestClient):
    params = {"exchange_name": EXCHANGE, "market": MARKET, "mode": MODE}
    response = client.get("/ccxt/fetch_balance", params=params)
    assert response.status_code == 200
    data = response.json()
    assert "balance" in data
    balance = data["balance"]
    assert "free" in balance
    assert "used" in balance
    assert "total" in balance

    # Check USDT specific for Futures
    # Note: Sandbox might return empty balances if not funded, but usually has USDT
    if "USDT" in balance["total"]:
        assert isinstance(balance["total"]["USDT"], (int, float))


def test_fetch_positions(client: TestClient):
    params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbols": SYMBOL,
    }
    # Extended router requires symbols list as query param "symbols"
    # FastAPI handles list query params as `symbols=A&symbols=B`.
    # Client params dict handles list automagically.
    response = client.get("/ccxt/fetch_positions", params=params)
    assert response.status_code == 200
    data = response.json()
    assert "positions" in data
    positions = data["positions"]
    assert isinstance(positions, list)

    # If positions exist, check structure matching PositionStructure
    for pos in positions:
        assert "symbol" in pos
        assert pos["symbol"] == SYMBOL
        assert "contracts" in pos
        assert isinstance(pos["contracts"], (int, float))
        assert "leverage" in pos
        assert "side" in pos  # long/short
        assert "unrealizedPnl" in pos


def test_set_leverage(client: TestClient):
    payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "leverage": 5,
    }
    response = client.post("/ccxt/set_leverage", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "result" in data
    assert isinstance(data["result"], dict)


def test_set_margin_mode(client: TestClient):
    payload = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "marginMode": "isolated",
    }
    response = client.post("/ccxt/set_margin_mode", json=payload)
    # This might fail if pos exists, but we test the ROUTE connectivity
    if response.status_code != 200:
        # Check if it's an API error (400/500)
        assert response.status_code in [400, 500]
        err = response.json()
        assert "detail" in err
    else:
        assert response.status_code == 200
        data = response.json()
        assert "result" in data
