import asyncio
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from src.cache_tool import OhlcvResult
from src.router.auth_handler import manager
from src.router.trader_router import ccxt_router
from src.tools.exchange_manager import exchange_manager


class FakeOhlcvClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _result(self) -> OhlcvResult:
        return OhlcvResult([(60_000, 1.0, 2.0, 0.0, 1.5, 5.0)], False)

    def fetch_ohlcv_since_limit(self, *args, **kwargs):
        self.calls.append(("since-limit", args, kwargs))
        return self._result()

    def fetch_ohlcv_since_latest(self, *args, **kwargs):
        self.calls.append(("since-latest", args, kwargs))
        return self._result()

    def fetch_ohlcv_latest_limit(self, *args, **kwargs):
        self.calls.append(("latest-limit", args, kwargs))
        return self._result()

    def fetch_positions(self, symbols):
        self.calls.append(("positions", (symbols,), {}))
        return []


@pytest.fixture
def ohlcv_http_client(monkeypatch):
    fake = FakeOhlcvClient()
    identities: list[tuple[str, str, str]] = []

    def get_client(exchange_name, market, mode):
        identities.append((exchange_name, market, mode))
        return fake

    monkeypatch.setattr(exchange_manager, "get_client", get_client)
    app = FastAPI()
    app.include_router(ccxt_router)
    app.dependency_overrides[manager] = lambda: {"sub": "test"}
    return app, fake, identities


def _get(
    app: FastAPI,
    path: str,
    params: Any,
) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.get(path, params=params)

    return asyncio.run(request())


def _base_query() -> dict[str, str]:
    return {
        "exchange_name": "binance",
        "market": "future",
        "mode": "sandbox",
        "symbol": "BTC/USDT:USDT",
        "timeframe": "1m",
        "variant": "mark",
        "enable_cache": "false",
    }


@pytest.mark.parametrize(
    ("path", "extra", "operation"),
    [
        (
            "/ccxt/fetch_ohlcv/since-limit",
            {"since": "1785542400000", "limit": "2"},
            "since-limit",
        ),
        (
            "/ccxt/fetch_ohlcv/since-latest",
            {"since": "1785542400000"},
            "since-latest",
        ),
        (
            "/ccxt/fetch_ohlcv/latest-limit",
            {"limit": "2"},
            "latest-limit",
        ),
    ],
)
def test_three_http_routes_parse_query_and_dispatch(
    ohlcv_http_client, path, extra, operation
):
    app, fake, identities = ohlcv_http_client

    response = _get(app, path, _base_query() | extra)

    assert response.status_code == 200
    assert response.json() == {
        "rows": [[60_000, 1.0, 2.0, 0.0, 1.5, 5.0]],
        "last_bar_completion_confirmed": False,
    }
    assert identities == [("binance", "future", "sandbox")]
    assert fake.calls[0][0] == operation
    assert fake.calls[0][2] == {
        "variant": "mark",
        "enable_cache": False,
    }


@pytest.mark.parametrize(
    ("path", "params"),
    [
        (
            "/ccxt/fetch_ohlcv/since-limit",
            {"since": "1785542400000", "limit": "0"},
        ),
        (
            "/ccxt/fetch_ohlcv/since-limit",
            {"since": "1785542400000", "limit": "100001"},
        ),
        ("/ccxt/fetch_ohlcv/latest-limit", {"limit": "0"}),
        ("/ccxt/fetch_ohlcv/latest-limit", {"limit": "100001"}),
    ],
)
def test_count_bounds_fail_before_provider_call(ohlcv_http_client, path, params):
    app, fake, identities = ohlcv_http_client

    response = _get(app, path, _base_query() | params)

    assert response.status_code == 422
    assert fake.calls == []
    assert identities == []


@pytest.mark.parametrize("since", ["178554240000", "17855424000000"])
def test_since_rejects_non_millisecond_digit_width(ohlcv_http_client, since):
    app, fake, identities = ohlcv_http_client

    response = _get(
        app,
        "/ccxt/fetch_ohlcv/since-limit",
        _base_query() | {"since": since, "limit": "2"},
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["query", "since"]
    assert fake.calls == []
    assert identities == []


@pytest.mark.parametrize("unknown", ["include_last", "enable_cach", "typo"])
def test_unknown_ohlcv_query_parameter_is_rejected(ohlcv_http_client, unknown):
    app, fake, identities = ohlcv_http_client
    params = _base_query() | {"limit": "2", unknown: "false"}

    response = _get(app, "/ccxt/fetch_ohlcv/latest-limit", params)

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "extra_forbidden"
    assert response.json()["detail"][0]["loc"] == ["query", unknown]
    assert fake.calls == []
    assert identities == []


def test_fetch_positions_symbols_are_repeated_query_parameters(ohlcv_http_client):
    app, fake, identities = ohlcv_http_client
    params = [
        ("exchange_name", "binance"),
        ("market", "future"),
        ("mode", "live"),
        ("symbols", "BTC/USDT:USDT"),
        ("symbols", "ETH/USDT:USDT"),
    ]

    response = _get(app, "/ccxt/fetch_positions", params)

    assert response.status_code == 200
    assert response.json() == {"positions": []}
    assert identities == [("binance", "future", "live")]
    assert fake.calls == [("positions", (["BTC/USDT:USDT", "ETH/USDT:USDT"],), {})]


@pytest.mark.parametrize(
    "path",
    [
        "/ccxt/ohlcv/since-limit",
        "/ccxt/ohlcv/since-latest",
        "/ccxt/ohlcv/latest-limit",
        "/ccxt/fetch_ohlcv",
    ],
)
def test_removed_ohlcv_routes_return_404(ohlcv_http_client, path):
    app, fake, identities = ohlcv_http_client

    response = _get(app, path, _base_query())

    assert response.status_code == 404
    assert fake.calls == []
    assert identities == []
