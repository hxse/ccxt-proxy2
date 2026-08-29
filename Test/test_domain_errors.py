import asyncio
import json

import ccxt
import pytest
from starlette.requests import Request

from src.domain_errors import CacheCapacityExceeded
from src.tools.ccxt_errors import map_ccxt_exception
from src.tools.shared import handle_ccxt_exception, handle_domain_error


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/ccxt/fetch_ohlcv/since-limit",
            "headers": [],
            "query_string": b"",
            "client": ("test", 1),
            "server": ("test", 80),
            "scheme": "http",
            "root_path": "",
        }
    )


def test_cache_capacity_error_becomes_request_scoped_http_507():
    request = _request()

    response = asyncio.run(
        handle_domain_error(request, CacheCapacityExceeded("eviction failed"))
    )

    assert response.status_code == 507
    assert json.loads(response.body) == {
        "detail": {
            "code": "CACHE_CAPACITY_EXCEEDED",
            "message": "eviction failed",
        }
    }


@pytest.mark.parametrize(
    ("exception", "status_code", "code"),
    [
        (ccxt.BadSymbol("bad"), 422, "INVALID_PROVIDER_REQUEST"),
        (ccxt.OrderNotFound("missing"), 404, "ORDER_NOT_FOUND"),
        (ccxt.InvalidOrder("bad order"), 422, "ORDER_REJECTED"),
        (ccxt.InsufficientFunds("funds"), 409, "INSUFFICIENT_FUNDS"),
        (ccxt.OperationRejected("rejected"), 409, "PROVIDER_OPERATION_REJECTED"),
        (ccxt.AuthenticationError("secret detail"), 502, "PROVIDER_AUTH_FAILED"),
        (ccxt.CancelPending("pending"), 502, "OPERATION_STATUS_UNKNOWN"),
        (ccxt.NotSupported("unsupported"), 422, "NOT_SUPPORTED"),
        (ccxt.NetworkError("offline"), 502, "NETWORK_INCOMPLETE"),
        (ccxt.ExchangeError("upstream"), 502, "PROVIDER_FAILURE"),
    ],
)
def test_ccxt_error_taxonomy(exception, status_code, code):
    mapped = map_ccxt_exception(exception)

    assert mapped.status_code == status_code
    assert mapped.code == code


def test_ccxt_exception_handler_returns_stable_sanitized_error():
    response = asyncio.run(
        handle_ccxt_exception(_request(), ccxt.AuthenticationError("secret detail"))
    )

    assert response.status_code == 502
    assert json.loads(response.body) == {
        "detail": {
            "code": "PROVIDER_AUTH_FAILED",
            "message": "provider authentication or permission check failed",
        }
    }
    assert b"secret detail" not in response.body
