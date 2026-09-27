"""真实 CCXT 编解码的离线样本；仅替换 markets、行情和网络边界。"""

from urllib.parse import parse_qs, urlsplit

import ccxt
import pytest
from fastapi import FastAPI

from src.domain_errors import DomainError
from src.router.auth_handler import manager as auth_manager
from src.router.trader_router import ccxt_router
from src.tools.ccxt_client import CcxtClient
from src.tools.shared import handle_domain_error
from Test.test_ctp_http import LocalClient

STAMP = "2026-09-24T08:00:00.000Z"
ORDER_ID = "00000000-0000-4000-8000-000000000001"


def sdk_client(monkeypatch, provider, reply):
    factory = ccxt.krakenfutures if provider == "kraken" else ccxt.binance
    sdk = factory(
        {"apiKey": "offline", "secret": "b2ZmbGluZQ==", "enableRateLimit": False}
    )
    symbol = "BTC/USD:USD" if provider == "kraken" else "BTC/USDT:USDT"
    market_id = "PF_XBTUSD" if provider == "kraken" else "BTCUSDT"
    sdk.set_markets(
        [
            {
                "id": market_id,
                "symbol": symbol,
                "base": "BTC",
                "quote": "USD" if provider == "kraken" else "USDT",
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
                "limits": {"amount": {"min": 0.001}},
                "info": {
                    "orderTypes": ["LIMIT", "MARKET"],
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
        ]
    )
    calls = []

    def fetch(url, method="GET", headers=None, body=None):
        target = urlsplit(url)
        params = {
            key: values[-1]
            for key, values in parse_qs(target.query or body or "").items()
        }
        calls.append((method, target.path, params))
        return reply(method, target.path, params)

    monkeypatch.setattr(sdk, "fetch", fetch)
    monkeypatch.setattr(
        sdk,
        "fetch_ticker",
        lambda symbol: {"markPrice": 50000, "bid": 49999, "ask": 50001},
    )
    monkeypatch.setattr(
        sdk, "fetch_currencies", lambda *a, **kw: pytest.fail("markets 已固定")
    )
    client = CcxtClient(sdk, provider, "future", "live", None)
    return client, calls, symbol


def route_client(monkeypatch, client):
    monkeypatch.setattr(
        "src.router.trader_router.exchange_manager.get_client", lambda *args: client
    )
    app = FastAPI()
    app.include_router(ccxt_router)
    app.exception_handler(DomainError)(handle_domain_error)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "offline"}
    return LocalClient(app), app


def kraken_send_reply(method, path, params):
    # 据官方 sendorder/CCXT 样本结构改写 ID、交易对和数值；不是账户历史记录。
    assert method == "POST" and path.endswith("/sendorder")
    kind = params["orderType"]
    assert kind in {"lmt", "post", "ioc", "fok"}
    detail = {
        "orderId": ORDER_ID,
        "cliOrdId": params.get("cliOrdId"),
        "type": kind,
        "symbol": params["symbol"],
        "side": params["side"],
        "quantity": float(params["size"]),
        "filled": 0,
        "limitPrice": float(params["limitPrice"]),
        "reduceOnly": False,
        "timestamp": STAMP,
        "lastUpdateTimestamp": STAMP,
    }
    immediate = kind in {"ioc", "fok"}
    event = (
        {
            "type": "EXECUTION",
            "executionId": "offline-fill",
            "price": detail["limitPrice"],
            "amount": detail["quantity"],
            "orderPriorEdit": None,
            "orderPriorExecution": detail,
            "takerReducedQuantity": None,
        }
        if immediate
        else {"type": "PLACE", "order": detail, "reducedQuantity": None}
    )
    return {
        "result": "success",
        "serverTime": STAMP,
        "sendStatus": {
            "order_id": ORDER_ID,
            "status": "filled" if immediate else "placed",
            "receivedTime": STAMP,
            "orderEvents": [event],
        },
    }
