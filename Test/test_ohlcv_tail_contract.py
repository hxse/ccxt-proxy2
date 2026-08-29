from Test.test_ccxt_client import MINUTE, _client, _minutes


def _cached_times(client, since: int = MINUTE) -> list[int]:
    series = client._series("BTC/USDT:USDT", "1m", "default")
    return [row[0] for row in client.cache.read_best_prefix(series.key, since, None)]


def test_since_limit_count_includes_unknown_tail_but_cache_omits_it(temp_dir):
    client, _ = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))

    result = client.fetch_ohlcv_since_limit(
        "BTC/USDT:USDT", "1m", MINUTE, 5
    )

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4, 5)
    assert result.last_bar_completion_confirmed is False
    assert _cached_times(client) == _minutes(1, 2, 3, 4)


def test_since_limit_successor_confirms_and_caches_requested_tail(temp_dir):
    client, _ = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))

    result = client.fetch_ohlcv_since_limit(
        "BTC/USDT:USDT", "1m", MINUTE, 4
    )

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4)
    assert result.last_bar_completion_confirmed is True
    assert _cached_times(client) == _minutes(1, 2, 3, 4)


def test_since_latest_returns_snapshot_tail_but_does_not_cache_it(temp_dir):
    client, _ = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))

    result = client.fetch_ohlcv_since_latest(
        "BTC/USDT:USDT", "1m", MINUTE
    )

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4, 5)
    assert result.last_bar_completion_confirmed is False
    assert _cached_times(client) == _minutes(1, 2, 3, 4)


def test_latest_limit_returns_full_limit_while_cache_omits_unknown_tail(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))

    result = client.fetch_ohlcv_latest_limit(
        "BTC/USDT:USDT",
        "1m",
        3,
    )

    assert [row[0] for row in result.rows] == _minutes(3, 4, 5)
    assert result.last_bar_completion_confirmed is False
    assert exchange.ohlcv_calls[0]["limit"] == 3
    assert _cached_times(client, 3 * MINUTE) == _minutes(3, 4)


def test_since_later_than_latest_returns_empty_without_creating_cache(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3))

    result = client.fetch_ohlcv_since_latest(
        "BTC/USDT:USDT", "1m", 4 * MINUTE
    )

    assert result.rows == []
    assert result.last_bar_completion_confirmed is None
    assert len(exchange.ohlcv_calls) == 1
    count = (
        client.cache._connection()
        .execute("SELECT COUNT(*) FROM cache_segments")
        .fetchone()[0]
    )
    assert count == 0
