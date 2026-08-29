import asyncio
import json

from starlette.requests import Request

from src.domain_errors import CacheCapacityExceeded
from src.tools.shared import handle_domain_error


def test_cache_capacity_error_becomes_request_scoped_http_507():
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/ccxt/ohlcv/since-limit",
            "headers": [],
            "query_string": b"",
            "client": ("test", 1),
            "server": ("test", 80),
            "scheme": "http",
            "root_path": "",
        }
    )

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
