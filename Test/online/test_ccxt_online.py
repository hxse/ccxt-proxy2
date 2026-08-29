import math
import os

import pytest

from src.tools.exchange_manager import exchange_manager
from src.tools.shared import config

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("CCXT_ONLINE") != "1",
        reason="CCXT online tests require CCXT_ONLINE=1 and configured credentials",
    ),
]

FUTURE_SYMBOLS = {
    "binance": "BTC/USDT:USDT",
    "kraken": "BTC/USD:USD",
}


@pytest.fixture(scope="module")
def future_clients(tmp_path_factory):
    cache_path = tmp_path_factory.mktemp("ccxt-online") / "ohlcv.duckdb"
    online_config = config.model_copy(
        update={
            "ohlcv_cache": config.ohlcv_cache.model_copy(
                update={"database_path": str(cache_path)}
            )
        }
    )
    futures = [
        item for item in online_config.exchange_whitelist if item.market == "future"
    ]
    if not futures:
        pytest.skip("no futures provider is enabled")
    exchange_manager.init_from_config(online_config)
    clients = [
        (
            item.exchange,
            item.mode,
            FUTURE_SYMBOLS[item.exchange],
            exchange_manager.get_client(item.exchange, item.market, item.mode),
        )
        for item in futures
    ]
    try:
        yield clients
    finally:
        exchange_manager.close()


def _one_private_identity_per_provider(future_clients):
    selected = {}
    for provider, mode, symbol, client in future_clients:
        if provider not in selected or mode == "sandbox":
            selected[provider] = (provider, mode, symbol, client)
    return list(selected.values())


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


def test_future_providers_return_canonical_latest_rows(future_clients):
    for provider, mode, symbol, client in future_clients:
        result = client.fetch_ohlcv_latest_limit(
            symbol,
            "1m",
            3,
            enable_cache=False,
        )

        _assert_rows(result.rows, 3)
        assert result.last_bar_completion_confirmed is False, f"{provider}/{mode}"


def test_three_ohlcv_modes_preserve_count_start_and_snapshot_semantics(
    future_clients,
):
    for provider, mode, symbol, client in future_clients:
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


def test_binance_read_only_price_variants(future_clients):
    for provider, _, symbol, client in future_clients:
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


def test_public_ticker_reads_are_available_for_every_identity(future_clients):
    for provider, mode, symbol, client in future_clients:
        tickers = client.fetch_tickers([symbol])

        assert symbol in tickers, f"{provider}/{mode}"
        assert tickers[symbol].get("symbol") == symbol, f"{provider}/{mode}"


def test_one_identity_per_provider_supports_private_account_reads(future_clients):
    for provider, mode, symbol, client in _one_private_identity_per_provider(
        future_clients
    ):
        balance = client.fetch_balance()
        positions = client.fetch_positions([symbol])
        market = client.fetch_market_info(symbol)

        assert isinstance(balance, dict), f"{provider}/{mode}"
        assert {"free", "used", "total"}.issubset(balance), f"{provider}/{mode}"
        assert isinstance(positions, list), f"{provider}/{mode}"
        assert market["symbol"] == symbol
        assert market["linear"] is True
        assert market["contract_size"] > 0
        assert market["precision_amount"] >= 0
        assert market["min_amount"] >= 0
        assert market["leverage"] is None or market["leverage"] > 0


def test_order_and_trade_history_reads_do_not_mutate_account(future_clients):
    for provider, mode, symbol, client in _one_private_identity_per_provider(
        future_clients
    ):
        open_orders = client.fetch_open_orders(symbol, None, 2)
        closed_orders = client.fetch_closed_orders(symbol, None, 2)
        trades = client.fetch_my_trades(symbol, None, 2)

        assert isinstance(open_orders, list), f"{provider}/{mode}"
        assert isinstance(closed_orders, list), f"{provider}/{mode}"
        assert isinstance(trades, list), f"{provider}/{mode}"
