import inspect
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.cache_tool import OhlcvResult
from src.router import trader_router
from src.tools.ccxt_client import CcxtClient
from src.tools.exchange_manager import exchange_manager
from src.types import (
    LatestLimitOhlcvRequest,
    SinceLatestOhlcvRequest,
    SinceLimitOhlcvRequest,
    StopMarketOrderRequest,
)


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def _result(self):
        return OhlcvResult([(10, 1.0, 2.0, 0.0, 1.5, 5.0)], False)

    def fetch_ohlcv_since_limit(self, *args, **kwargs):
        self.calls.append(("since-limit", args, kwargs))
        return self._result()

    def fetch_ohlcv_since_latest(self, *args, **kwargs):
        self.calls.append(("since-latest", args, kwargs))
        return self._result()

    def fetch_ohlcv_latest_limit(self, *args, **kwargs):
        self.calls.append(("latest-limit", args, kwargs))
        return self._result()

    def create_stop_market_order(self, *args, **kwargs):
        self.calls.append(("stop-market", args, kwargs))
        return {"id": "stop"}


def _base() -> dict[str, Any]:
    return {
        "exchange_name": "binance",
        "market": "future",
        "mode": "sandbox",
        "symbol": "BTC/USDT",
        "timeframe": "1m",
    }


def test_three_ohlcv_routes_call_the_three_client_methods(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(exchange_manager, "get_client", lambda *args: fake)

    first = trader_router.fetch_ohlcv_since_limit(
        SinceLimitOhlcvRequest(**_base(), since=10, limit=2)
    )
    second = trader_router.fetch_ohlcv_since_latest(
        SinceLatestOhlcvRequest(**_base(), since=10)
    )
    third = trader_router.fetch_ohlcv_latest_limit(
        LatestLimitOhlcvRequest(**_base(), limit=2)
    )

    assert [call[0] for call in fake.calls] == [
        "since-limit",
        "since-latest",
        "latest-limit",
    ]
    assert first.model_dump() == second.model_dump() == third.model_dump()


def test_router_exposes_only_the_three_unambiguous_ohlcv_paths():
    paths = {getattr(route, "path", None) for route in trader_router.ccxt_router.routes}

    assert "/ccxt/fetch_ohlcv/since-limit" in paths
    assert "/ccxt/fetch_ohlcv/since-latest" in paths
    assert "/ccxt/fetch_ohlcv/latest-limit" in paths
    assert "/ccxt/fetch_ohlcv" not in paths
    assert "/ccxt/ohlcv/since-limit" not in paths
    assert "/ccxt/ohlcv/since-latest" not in paths
    assert "/ccxt/ohlcv/latest-limit" not in paths


def test_openapi_contains_three_distinct_ohlcv_query_schemas():
    app = FastAPI()
    app.include_router(trader_router.ccxt_router)

    paths = app.openapi()["paths"]

    assert "/ccxt/fetch_ohlcv/since-limit" in paths
    assert "/ccxt/fetch_ohlcv/since-latest" in paths
    assert "/ccxt/fetch_ohlcv/latest-limit" in paths
    for path in (
        "/ccxt/fetch_ohlcv/since-limit",
        "/ccxt/fetch_ohlcv/since-latest",
        "/ccxt/fetch_ohlcv/latest-limit",
    ):
        parameters = {item["name"] for item in paths[path]["get"]["parameters"]}
        assert "include_last" not in parameters


@pytest.mark.parametrize(
    "request_type",
    [SinceLimitOhlcvRequest, SinceLatestOhlcvRequest, LatestLimitOhlcvRequest],
)
def test_ohlcv_request_models_do_not_expose_include_last(request_type):
    assert "include_last" not in request_type.model_fields


@pytest.mark.parametrize(
    "method",
    [
        CcxtClient.fetch_ohlcv_since_limit,
        CcxtClient.fetch_ohlcv_since_latest,
        CcxtClient.fetch_ohlcv_latest_limit,
    ],
)
def test_ohlcv_client_methods_do_not_expose_include_last(method):
    assert "include_last" not in inspect.signature(method).parameters


def test_route_forwards_cache_and_variant_flags(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(exchange_manager, "get_client", lambda *args: fake)
    request = SinceLimitOhlcvRequest(
        **_base(),
        since=10,
        limit=2,
        variant="mark",
        enable_cache=False,
    )

    trader_router.fetch_ohlcv_since_limit(request)

    assert fake.calls[0][2] == {
        "variant": "mark",
        "enable_cache": False,
    }


@pytest.mark.parametrize(
    "request_type", [SinceLimitOhlcvRequest, LatestLimitOhlcvRequest]
)
def test_count_routes_reject_100001_rows(request_type):
    parameters = _base() | {"limit": 100_001}
    if request_type is SinceLimitOhlcvRequest:
        parameters["since"] = 10

    with pytest.raises(ValidationError):
        request_type(**parameters)


def test_trigger_route_delegates_ccxt_translation_to_client(monkeypatch):
    fake = FakeClient()
    monkeypatch.setattr(exchange_manager, "get_client", lambda *args: fake)
    parameters = _base()
    parameters.pop("timeframe")
    request = StopMarketOrderRequest(
        **parameters,
        side="sell",
        amount=0.1,
        triggerPrice=50_000,
        reduceOnly=True,
    )

    trader_router.create_stop_market_order(request)

    assert fake.calls[0][0] == "stop-market"
    assert fake.calls[0][2]["reduce_only"] is True
