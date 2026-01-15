from fastapi.testclient import TestClient

EXCHANGE = "binance"
MARKET = "future"
MODE = "sandbox"
SYMBOL = "BTC/USDT:USDT"


def test_fetch_tickers(client: TestClient):
    params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbols": SYMBOL,
    }
    response = client.get("/ccxt/fetch_tickers", params=params)
    assert response.status_code == 200
    data = response.json()
    assert "tickers" in data
    tickers = data["tickers"]
    assert isinstance(tickers, dict)

    # Check specific symbol ticker
    if SYMBOL in tickers:
        ticker = tickers[SYMBOL]
        assert "symbol" in ticker
        assert "last" in ticker
        assert "timestamp" in ticker
        assert isinstance(ticker["last"], (int, float))
        assert ticker["symbol"] == SYMBOL


def test_fetch_ohlcv(client: TestClient):
    params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
        "timeframe": "1h",
        "limit": 5,
    }
    response = client.get("/ccxt/fetch_ohlcv", params=params)
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

    if len(data) > 0:
        candle = data[0]
        assert isinstance(candle, list)
        assert len(candle) == 6  # [time, open, high, low, close, vol]
        assert isinstance(candle[0], (int, float))  # timestamp
        assert isinstance(candle[4], (int, float))  # close price


def test_fetch_market_info(client: TestClient):
    params = {
        "exchange_name": EXCHANGE,
        "market": MARKET,
        "mode": MODE,
        "symbol": SYMBOL,
    }
    response = client.get("/ccxt/fetch_market_info", params=params)
    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == SYMBOL
    assert "contract_size" in data
    assert "precision_amount" in data
    assert "min_amount" in data

    # Logic checks
    assert data["contract_size"] > 0
    assert data["precision_amount"] > 0
    assert data["min_amount"] >= 0
