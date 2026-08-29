import os

import pytest

from src.tools.exchange_manager import exchange_manager
from src.tools.shared import config

pytestmark = pytest.mark.skipif(
    os.getenv("CCXT_ONLINE") != "1",
    reason="CCXT online tests require CCXT_ONLINE=1 and configured credentials",
)

FUTURE_SYMBOLS = {
    "binance": "BTC/USDT:USDT",
    "kraken": "BTC/USD:USD",
}


def test_enabled_future_providers_support_three_read_only_ohlcv_modes():
    futures = [item for item in config.exchange_whitelist if item.market == "future"]
    if not futures:
        pytest.skip("no futures provider is enabled")
    exchange_manager.init_from_config(config)

    for item in futures:
        client = exchange_manager.get_client(item.exchange, item.market, item.mode)
        symbol = FUTURE_SYMBOLS[item.exchange]
        latest = client.fetch_ohlcv_latest_limit(
            symbol, "1m", 3, enable_cache=False
        )
        assert latest.rows, f"{item.exchange} LatestLimit returned no rows"
        since = latest.rows[0][0]

        counted = client.fetch_ohlcv_since_limit(
            symbol, "1m", since, 2, enable_cache=False
        )
        assert counted.rows, f"{item.exchange} SinceLimit returned no rows"
        assert counted.rows[0][0] >= since

        snapshot = client.fetch_ohlcv_since_latest(
            symbol, "1m", since, enable_cache=False
        )
        assert snapshot.rows, f"{item.exchange} SinceLatest returned no rows"
        assert snapshot.rows[0][0] >= since
