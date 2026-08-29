import math
import os

import pytest

from src.tools.exchange_manager import exchange_manager
from src.tools.shared import config

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("CCXT_ONLINE") != "1",
        reason="CCXT online tests require CCXT_ONLINE=1 and configured live identities",
    ),
]

FUTURE_SYMBOLS = {
    "binance": "BTC/USDT:USDT",
    "kraken": "BTC/USD:USD",
}


@pytest.fixture(scope="module")
def live_future_clients(tmp_path_factory):
    cache_path = tmp_path_factory.mktemp("ccxt-online") / "ohlcv.duckdb"
    live_futures = [
        item
        for item in config.exchange_whitelist
        if item.market == "future" and item.mode == "live"
    ]
    if not live_futures:
        pytest.skip("no live futures provider is enabled")
    online_config = config.model_copy(
        update={
            "exchange_whitelist": live_futures,
            "ohlcv_cache": config.ohlcv_cache.model_copy(
                update={"database_path": str(cache_path)}
            ),
        }
    )
    exchange_manager.init_from_config(online_config)
    clients = [
        (
            item.exchange,
            item.mode,
            FUTURE_SYMBOLS[item.exchange],
            exchange_manager.get_client(item.exchange, item.market, item.mode),
        )
        for item in live_futures
    ]
    try:
        yield clients
    finally:
        exchange_manager.close()


def _assert_rows(rows, expected_count: int | None = None) -> None:
    assert rows
    if expected_count is not None:
        assert len(rows) == expected_count
    timestamps = [row[0] for row in rows]
    assert timestamps == sorted(set(timestamps))
    for timestamp, open_, high, low, close, volume in rows:
        assert isinstance(timestamp, int)
        assert all(math.isfinite(value) for value in (open_, high, low, close, volume))
        assert high >= max(open_, close)
        assert low <= min(open_, close)
        assert volume >= 0


def test_live_future_providers_return_canonical_latest_rows(live_future_clients):
    for provider, mode, symbol, client in live_future_clients:
        assert mode == "live"
        result = client.fetch_ohlcv_latest_limit(
            symbol,
            "1m",
            3,
            enable_cache=False,
        )

        _assert_rows(result.rows, 3)
        assert result.last_bar_completion_confirmed is False, f"{provider}/{mode}"


def test_three_ohlcv_modes_preserve_count_start_and_snapshot_semantics(
    live_future_clients,
):
    for provider, mode, symbol, client in live_future_clients:
        latest = client.fetch_ohlcv_latest_limit(
            symbol,
            "1m",
            4,
            enable_cache=False,
        )
        _assert_rows(latest.rows, 4)
        since = latest.rows[0][0]

        counted = client.fetch_ohlcv_since_limit(
            symbol,
            "1m",
            since,
            3,
            enable_cache=False,
        )
        _assert_rows(counted.rows, 3)
        assert counted.rows[0][0] == since, f"{provider}/{mode}"
        assert counted.last_bar_completion_confirmed is True, f"{provider}/{mode}"

        snapshot = client.fetch_ohlcv_since_latest(
            symbol,
            "1m",
            since,
            enable_cache=False,
        )
        _assert_rows(snapshot.rows)
        assert snapshot.rows[0][0] == since, f"{provider}/{mode}"
        assert snapshot.rows[-1][0] >= counted.rows[-1][0], f"{provider}/{mode}"
        assert isinstance(snapshot.last_bar_completion_confirmed, bool)


def test_binance_read_only_price_variants(live_future_clients):
    for provider, _, symbol, client in live_future_clients:
        if provider != "binance":
            continue
        for variant in ("mark", "index", "premiumIndex"):
            result = client.fetch_ohlcv_latest_limit(
                symbol,
                "1m",
                2,
                variant=variant,
                enable_cache=False,
            )
            _assert_rows(result.rows, 2)
            assert result.last_bar_completion_confirmed is False


def test_public_ticker_reads_are_available_for_every_live_identity(
    live_future_clients,
):
    for provider, mode, symbol, client in live_future_clients:
        tickers = client.fetch_tickers([symbol])

        assert symbol in tickers, f"{provider}/{mode}"
        assert tickers[symbol].get("symbol") == symbol, f"{provider}/{mode}"
