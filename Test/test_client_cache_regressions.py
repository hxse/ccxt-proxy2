from src.cache_tool import OhlcvResult
from src.cache_tool.models import canonical_row
from Test.test_ccxt_client import MINUTE, _client, _minutes, _row

SYMBOL = "BTC/USDT:USDT"


def _cached_rows(client, since: int = MINUTE):
    series = client._series(SYMBOL, "1m", "default")
    return client.cache.read_best_prefix(series.key, since, None)


def test_client_reuses_leading_gap_proof_for_a_later_since(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(3, 4, 5))

    first = client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 2)
    calls_after_first = len(exchange.ohlcv_calls)
    second = client.fetch_ohlcv_since_limit(SYMBOL, "1m", 2 * MINUTE, 2)

    assert [row[0] for row in first.rows] == _minutes(3, 4)
    assert [row[0] for row in second.rows] == _minutes(3, 4)
    assert len(exchange.ohlcv_calls) == calls_after_first


def test_partial_cache_extension_applies_provider_revision_on_overlap(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4))
    client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 2)
    revised = [2 * MINUTE, 200.0, 202.0, 198.0, 201.0, 10.0]
    exchange.rows = [_row(MINUTE), revised, _row(3 * MINUTE), _row(4 * MINUTE)]

    result = client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 3)

    response_revision = next(row for row in result.rows if row[0] == 2 * MINUTE)
    cache_revision = next(row for row in _cached_rows(client) if row[0] == 2 * MINUTE)
    assert response_revision[4] == 201.0
    assert cache_revision[4] == 201.0


def test_partial_extension_does_not_cache_unconfirmed_network_tail(temp_dir):
    client, _ = _client(temp_dir, times=_minutes(1, 2, 3))
    client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 2)

    result = client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 3)

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3)
    assert result.last_bar_completion_confirmed is False
    assert [row[0] for row in _cached_rows(client)] == _minutes(1, 2)


def test_since_latest_overlap_failure_discards_prefix_and_refetches(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))
    client.fetch_ohlcv_since_limit(SYMBOL, "1m", MINUTE, 2)
    original_fetch = exchange.fetch_ohlcv
    omitted = False

    def omit_cached_anchor_once(*args, **kwargs):
        nonlocal omitted
        rows = original_fetch(*args, **kwargs)
        if kwargs.get("since") == 2 * MINUTE and not omitted:
            omitted = True
            return [row for row in rows if row[0] != 2 * MINUTE]
        return rows

    exchange.fetch_ohlcv = omit_cached_anchor_once
    exchange.ohlcv_calls.clear()

    result = client.fetch_ohlcv_since_latest(SYMBOL, "1m", MINUTE)

    since_calls = [call["since"] for call in exchange.ohlcv_calls]
    assert 2 * MINUTE in since_calls
    assert MINUTE in since_calls
    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4, 5)
    assert [row[0] for row in _cached_rows(client)] == _minutes(1, 2, 3, 4)


def test_since_latest_full_cache_hit_only_fetches_latest_anchor(temp_dir):
    client, exchange = _client(temp_dir, times=_minutes(1, 2, 3, 4, 5))
    series = client._series(SYMBOL, "1m", "default")
    client.cache.write_segment(
        series.key,
        OhlcvResult(
            [canonical_row(_row(time)) for time in _minutes(1, 2, 3, 4, 5)],
            True,
        ),
        MINUTE,
    )

    result = client.fetch_ohlcv_since_latest(SYMBOL, "1m", MINUTE)

    assert [row[0] for row in result.rows] == _minutes(1, 2, 3, 4, 5)
    assert result.last_bar_completion_confirmed is True
    assert len(exchange.ohlcv_calls) == 1
    assert exchange.ohlcv_calls[0]["since"] is None
