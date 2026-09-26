import math
import os

import pytest

from src.tools.shared import config
from Test.online.live_service import LiveService

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("CCXT_ONLINE") != "1", reason="manual live-only entry required"
    ),
]
FUTURE_SYMBOLS = {"binance": "BTC/USDT:USDT", "kraken": "BTC/USD:USD"}


@pytest.fixture(scope="module", params=list(FUTURE_SYMBOLS))
def live_future_service(request, tmp_path_factory):
    provider = request.param
    live_futures = [
        item
        for item in config.service_whitelist
        if item.service == "ccxt"
        and item.exchange == provider
        and item.market == "future"
        and item.mode == "live"
    ]
    if not live_futures:
        pytest.skip(f"no {provider} live futures identity is enabled")
    online_config = config.model_copy(update={"service_whitelist": live_futures})
    service = LiveService(
        online_config, live_futures, tmp_path_factory.mktemp(provider + "-live")
    )
    try:
        service.start()
        yield (
            service,
            {
                "exchange_name": provider,
                "market": "future",
                "mode": "live",
                "symbol": FUTURE_SYMBOLS[provider],
                "timeframe": "1m",
            },
        )
    finally:
        service.close()


def assert_rows(rows, count=None):
    assert rows
    if count is not None:
        assert len(rows) == count
    times = [row[0] for row in rows]
    assert times == sorted(set(times))
    assert all(b - a == 60000 for a, b in zip(times, times[1:]))
    for timestamp, open_, high, low, close, volume in rows:
        assert type(timestamp) is int
        assert all(math.isfinite(v) for v in (open_, high, low, close, volume))
        assert high >= max(open_, close) and low <= min(open_, close) and volume >= 0


def test_three_modes_tail_cache_and_trusted_history(live_future_service):
    service, params = live_future_service
    latest = service.get("/ccxt/fetch_ohlcv/latest-limit", **params, limit=4)
    assert_rows(latest["rows"], 4)
    assert latest["last_bar_completion_confirmed"] is False
    summary = service.get(
        "/cache/summary",
        provider=params["exchange_name"],
        symbol=params["symbol"],
        timeframe="1m",
    )
    assert summary["items"][0]["count"] == 3
    assert summary["items"][0]["end"] == latest["rows"][-2][0]
    since = latest["rows"][0][0]
    cached = service.get(
        "/ccxt/fetch_ohlcv/since-limit", **params, since=since, limit=3
    )
    assert_rows(cached["rows"], 3)
    assert cached["last_bar_completion_confirmed"] is True
    counted = service.get(
        "/ccxt/fetch_ohlcv/since-limit",
        **params,
        since=since,
        limit=3,
        enable_cache=False,
    )
    assert_rows(counted["rows"], 3)
    assert counted["last_bar_completion_confirmed"] is False
    snapshot = service.get("/ccxt/fetch_ohlcv/since-latest", **params, since=since)
    assert_rows(snapshot["rows"])
    assert snapshot["rows"][0][0] == since
    assert snapshot["rows"][-1][0] >= latest["rows"][-1][0]


def test_binance_public_price_variants(live_future_service):
    service, params = live_future_service
    if params["exchange_name"] != "binance":
        pytest.skip("price variants only supported by Binance")
    for variant in ("mark", "index", "premiumIndex"):
        result = service.get(
            "/ccxt/fetch_ohlcv/latest-limit",
            **params,
            limit=2,
            variant=variant,
            enable_cache=False,
        )
        assert_rows(result["rows"], 2)
        assert result["last_bar_completion_confirmed"] is False
