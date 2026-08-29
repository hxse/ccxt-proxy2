import ccxt
import pytest

from src.domain_errors import (
    CacheCapacityExceeded,
    CapabilityNotSupported,
    InvalidProviderRequest,
    NetworkIncomplete,
    OperationStatusUnknown,
    ProviderClientClosed,
)
from Test.test_ccxt_client import MINUTE, _client, _minutes


def test_non_read_operation_is_not_retried(temp_dir):
    client, exchange = _client(temp_dir)

    def fail(*args, **kwargs):
        exchange.create_calls += 1
        raise ccxt.NetworkError("timeout")

    exchange.create_order = fail

    with pytest.raises(OperationStatusUnknown):
        client.create_order("BTC/USDT", "market", "buy", 1)
    assert exchange.create_calls == 1


def test_read_operation_retries_once(temp_dir, monkeypatch):
    client, exchange = _client(temp_dir)
    attempts = 0

    def flaky(params):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ccxt.NetworkError("temporary")
        return {"total": {}}

    exchange.fetch_balance = flaky
    monkeypatch.setattr("src.tools.ccxt_transport.time.sleep", lambda _: None)

    assert client.fetch_balance() == {"total": {}}
    assert attempts == 2


def test_read_operation_failure_after_retry_is_upstream_502_domain_error(
    temp_dir, monkeypatch
):
    client, exchange = _client(temp_dir)
    attempts = 0

    def fail(params):
        nonlocal attempts
        attempts += 1
        raise ccxt.NetworkError("offline")

    exchange.fetch_balance = fail
    monkeypatch.setattr("src.tools.ccxt_transport.time.sleep", lambda _: None)

    with pytest.raises(NetworkIncomplete):
        client.fetch_balance()
    assert attempts == 2


@pytest.mark.parametrize(
    "error",
    [ccxt.AuthenticationError("bad credentials"), ccxt.BadRequest("bad request")],
)
def test_read_provider_rejections_are_not_retried(temp_dir, error):
    client, exchange = _client(temp_dir)
    attempts = 0

    def fail(params):
        nonlocal attempts
        attempts += 1
        raise error

    exchange.fetch_balance = fail

    with pytest.raises(type(error)):
        client.fetch_balance()
    assert attempts == 1


def test_write_business_rejection_is_not_mislabeled_as_unknown_status(temp_dir):
    client, exchange = _client(temp_dir)

    def reject(*args, **kwargs):
        exchange.create_calls += 1
        raise ccxt.InvalidOrder("rejected before submission")

    exchange.create_order = reject

    with pytest.raises(ccxt.InvalidOrder):
        client.create_order("BTC/USDT:USDT", "limit", "buy", 1, 50_000)
    assert exchange.create_calls == 1


def test_later_ohlcv_page_failure_returns_no_partial_result(temp_dir, monkeypatch):
    client, exchange = _client(temp_dir)
    original_fetch = exchange.fetch_ohlcv
    client._ohlcv.page_limit = 3

    def fail_later_page(*args, **kwargs):
        if kwargs.get("since") == 3 * MINUTE:
            raise ccxt.NetworkError("later page failed")
        return original_fetch(*args, **kwargs)

    exchange.fetch_ohlcv = fail_later_page
    monkeypatch.setattr("src.tools.ccxt_transport.time.sleep", lambda _: None)

    with pytest.raises(NetworkIncomplete):
        client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 4)
    count = (
        client.cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments")
        .fetchone()[0]
    )
    assert count == 0


def test_capability_failure_is_explicit(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.has["fetchBalance"] = False

    with pytest.raises(CapabilityNotSupported):
        client.fetch_balance()


@pytest.mark.parametrize("timeframe", ["3m", "1M"])
def test_unsupported_provider_timeframe_is_explicit(temp_dir, timeframe):
    client, exchange = _client(temp_dir, provider="kraken", market="future")
    exchange.timeframes = {"1m": "1m"}

    with pytest.raises(
        CapabilityNotSupported,
        match=rf"does not support {timeframe} OHLCV",
    ):
        client.fetch_ohlcv_latest_limit("BTC/USD:USD", timeframe, 2)


def test_binance_future_rejects_inverse_market(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.linear = False

    with pytest.raises(CapabilityNotSupported, match="linear markets only"):
        client.fetch_ohlcv_latest_limit("BTC/USD:BTC", "1m", 2)


def test_binance_future_rejects_inverse_before_trading_call(temp_dir):
    client, exchange = _client(temp_dir)
    exchange.linear = False

    with pytest.raises(CapabilityNotSupported, match="linear markets only"):
        client.create_order("BTC/USD:BTC", "market", "buy", 1)

    assert exchange.create_calls == 0


@pytest.mark.parametrize(
    "operation",
    [
        lambda client, symbol: client.fetch_tickers([symbol]),
        lambda client, symbol: client.fetch_market_info(symbol),
        lambda client, symbol: client.fetch_open_orders(symbol, None, None),
        lambda client, symbol: client.fetch_closed_orders(symbol, None, None),
        lambda client, symbol: client.fetch_order("id", symbol),
        lambda client, symbol: client.cancel_order("id", symbol),
        lambda client, symbol: client.cancel_all_orders(symbol),
        lambda client, symbol: client.fetch_my_trades(symbol, None, None),
        lambda client, symbol: client.fetch_positions([symbol]),
        lambda client, symbol: client.set_leverage(2, symbol),
        lambda client, symbol: client.set_margin_mode("cross", symbol),
    ],
)
def test_all_symbol_bearing_trading_methods_share_linear_scope(temp_dir, operation):
    client, exchange = _client(temp_dir)
    exchange.linear = False

    with pytest.raises(CapabilityNotSupported, match="linear markets only"):
        operation(client, "BTC/USD:BTC")


def test_unknown_symbol_is_a_stable_provider_request_error(temp_dir):
    client, exchange = _client(temp_dir, provider="kraken", market="future")
    exchange.invalid_symbols.add("UNKNOWN/USD:USD")

    with pytest.raises(InvalidProviderRequest, match="unknown provider symbol"):
        client.fetch_positions(["UNKNOWN/USD:USD"])


def test_client_rejects_unknown_ohlcv_variant(temp_dir):
    client, _ = _client(temp_dir)

    with pytest.raises(InvalidProviderRequest, match="unsupported OHLCV variant"):
        client.fetch_ohlcv_latest_limit("BTC/USDT:USDT", "1m", 2, variant="raw")


@pytest.mark.parametrize("variant", ["mark", "index", "premiumIndex"])
def test_binance_variants_reach_provider_and_use_separate_cache_series(
    temp_dir, variant
):
    client, exchange = _client(temp_dir)

    client.fetch_ohlcv_since_limit("BTC/USDT:USDT", "1m", MINUTE, 2, variant=variant)

    assert exchange.ohlcv_calls[0]["params"]["price"] == variant
    series = client._series("BTC/USDT:USDT", "1m", variant)
    assert len(client.cache.read_best_prefix(series.key, MINUTE, None)) == 2


def test_client_close_is_idempotent(temp_dir):
    client, exchange = _client(temp_dir)
    provider_called = False

    def fetch_balance(params):
        nonlocal provider_called
        provider_called = True
        return {"total": {}}

    exchange.fetch_balance = fetch_balance

    client.close()
    client.close()

    assert exchange.closed == 1
    with pytest.raises(ProviderClientClosed, match="client is closed"):
        client.fetch_balance()
    assert provider_called is False


class FailingCache:
    def __init__(self, read_error=None, write_error=None):
        self.read_error = read_error
        self.write_error = write_error

    def read_best_prefix(self, *args):
        if self.read_error:
            raise self.read_error
        return []

    def write_segment(self, *args):
        if self.write_error:
            raise self.write_error


def test_cache_read_and_ordinary_write_failures_do_not_hide_network_data(temp_dir):
    client, _ = _client(temp_dir)
    client.cache = FailingCache(
        read_error=RuntimeError("read"), write_error=RuntimeError("write")
    )

    result = client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)

    assert [row[0] for row in result.rows] == _minutes(1, 2)


def test_cache_capacity_failure_is_propagated_to_http_error_layer(temp_dir):
    client, _ = _client(temp_dir)
    client.cache = FailingCache(write_error=CacheCapacityExceeded("full"))

    with pytest.raises(CacheCapacityExceeded):
        client.fetch_ohlcv_since_limit("BTC/USDT", "1m", MINUTE, 2)
