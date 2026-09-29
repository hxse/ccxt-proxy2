import math
import os

import pytest

from src.tools.shared import config
from Test.online.live_service import LiveService

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("TQ_ONLINE") != "1", reason="manual TQ online entry required"
    ),
]
SYMBOL = "KQ.m@SHFE.rb"


@pytest.fixture(scope="module")
def live_tq_service(tmp_path_factory):
    identities = [item for item in config.service_whitelist if item.service == "tq"]
    if not identities:
        pytest.skip("TQ is not enabled in service_whitelist")
    service = LiveService(config, identities, tmp_path_factory.mktemp("tq-live"))
    try:
        yield service.start()
    finally:
        service.close()


def assert_rows(rows, count):
    assert 0 < len(rows) <= count
    times = [row["datetime"] for row in rows]
    assert times == sorted(set(times))
    ids = [row["id"] for row in rows]
    assert all(b - a == 1 for a, b in zip(ids, ids[1:]))
    for row in rows:
        prices = [row[key] for key in ("open", "high", "low", "close")]
        assert all(isinstance(v, (int, float)) and math.isfinite(v) for v in prices)
        assert row["high"] >= max(row["open"], row["close"])
        assert row["low"] <= min(row["open"], row["close"])


def test_main_weighted_and_cache_tail(live_tq_service):
    service = live_tq_service
    for symbol in (SYMBOL, SYMBOL.replace("KQ.m@", "KQ.i@")):
        rows = service.get(
            "/tq/fetch_ohlcv", symbol=symbol, duration_seconds=300, data_length=20
        )
        assert_rows(rows, 20)
        summary = service.get(
            "/cache/summary", is_live=True, provider="tq", symbol=symbol, timeframe="300s"
        )
        assert summary["items"][0]["count"] == len(rows) - 1
        assert summary["items"][0]["end"] == rows[-2]["datetime"]


def test_own_calendar_nodes_and_transition(live_tq_service):
    service = live_tq_service
    calendar = service.get(
        "/tq/fetch_trading_calendar", start_date="2021-02-01", end_date="2021-02-03"
    )
    assert [row["date"] for row in calendar] == [
        "2021-02-01",
        "2021-02-02",
        "2021-02-03",
    ]
    assert all(type(row["trading"]) is bool for row in calendar)
    rows = service.get(
        "/tq/fetch_ohlcv", symbol=SYMBOL, duration_seconds=300, data_length=10000
    )
    assert_rows(rows, 10000)
    result = service.get(
        "/tq/fetch_underlying_symbol",
        symbol=SYMBOL,
        start_time=rows[0]["datetime"],
        end_time=rows[-1]["datetime"],
        transition_timeframe="5m",
    )
    assert result["items"][0]["underlying_symbol"]
    assert result["history"]
    dates = [node["date"] for node in result["history"]]
    assert dates == sorted(set(dates))
    windows = [node["transition"] for node in result["history"] if node["transition"]]
    for window in windows:
        assert 0 < len(window) <= 10
        assert [row["datetime"] for row in window] == sorted(
            {row["datetime"] for row in window}
        )
        assert all(
            set(row)
            == {
                "datetime",
                "old_open",
                "old_high",
                "old_low",
                "old_close",
                "old_volume",
            }
            for row in window
        )
    if not windows:
        pytest.skip(
            "calendar and mapping verified; old contract window does not cover a provable transition"
        )


def test_bitumen_current_and_history_use_same_source(live_tq_service):
    service = live_tq_service
    symbol = "KQ.m@SHFE.bu"
    rows = service.get(
        "/tq/fetch_ohlcv",
        symbol=symbol,
        duration_seconds=300,
        data_length=1,
        enable_cache=False,
    )
    assert_rows(rows, 1)
    current = service.get(
        "/tq/fetch_underlying_symbol",
        symbol=symbol,
        enable_cache=False,
    )
    history = service.get(
        "/tq/fetch_underlying_symbol",
        symbol=symbol,
        start_time=rows[0]["datetime"],
        end_time=rows[-1]["datetime"],
        enable_cache=False,
    )
    assert current.get("history", []) == []
    assert len(current["items"]) == 1 and history["history"]
    assert current["items"] == history["items"]
    underlying = current["items"][0]["underlying_symbol"]
    assert underlying.startswith("SHFE.bu")
    assert history["history"][-1]["underlying_symbol"] == underlying
    assert (
        service.get(
            "/cache/summary", is_live=True, provider="tq", symbol=symbol, kind="main_mapping"
        )["items"]
        == []
    )
