import math
import os
from datetime import date

import pytest

from src.tools.shared import config
from src.tools.tq_manager import TqManager
from src.types_tq import (
    TqOhlcvRequest,
    TqTickRequest,
    TqTradingCalendarRequest,
    TqUnderlyingSymbolRequest,
)

tq_manager = TqManager(config.tq)

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.getenv("TQ_ONLINE") != "1",
        reason="TQ online tests require TQ_ONLINE=1 and configured TQ credentials",
    ),
]


@pytest.fixture(scope="module", autouse=True)
def initialized_tq():
    if not any(item.service == "tq" for item in config.service_whitelist):
        pytest.skip("tq is not enabled in service_whitelist")
    try:
        tq_manager.initialize()
        yield
    finally:
        tq_manager.close()


def test_tq_online_fetch_ohlcv_smoke():
    result = tq_manager.fetch_ohlcv(
        TqOhlcvRequest(
            symbol="KQ.m@SHFE.rb",
            duration_seconds=60,
            data_length=10,
        )
    )

    assert 0 < len(result) <= 10
    timestamps = [row["datetime"] for row in result]
    assert timestamps == sorted(set(timestamps))
    for row in result:
        prices = [row[name] for name in ("open", "high", "low", "close")]
        assert all(isinstance(value, (int, float)) for value in prices)
        assert all(math.isfinite(value) for value in prices)
        assert row["high"] >= max(row["open"], row["close"])
        assert row["low"] <= min(row["open"], row["close"])
        assert row["volume"] is None or row["volume"] >= 0


def test_tq_online_fetch_tick_smoke():
    result = tq_manager.fetch_tick(
        TqTickRequest(
            symbol="KQ.m@SHFE.rb",
            data_length=10,
        )
    )

    assert 0 < len(result) <= 10
    timestamps = [row["datetime"] for row in result]
    assert timestamps == sorted(set(timestamps))
    for row in result:
        prices = [
            row.get("last_price"),
            row.get("bid_price1"),
            row.get("ask_price1"),
        ]
        finite_prices = [
            value
            for value in prices
            if isinstance(value, (int, float)) and math.isfinite(value)
        ]
        assert finite_prices


def test_tq_online_fetch_underlying_symbol_smoke():
    result = tq_manager.fetch_underlying_symbol(
        TqUnderlyingSymbolRequest(symbol="KQ.m@SHFE.rb", n=3)
    )

    assert len(result.items) == 1
    item = result.items[0]
    assert item.symbol == "KQ.m@SHFE.rb"
    assert item.ins_class == "CONT"
    assert item.underlying_symbol
    assert len(result.history) <= 3
    assert all(history.underlying_symbol for history in result.history)


def test_tq_online_fetch_trading_calendar_smoke():
    result = tq_manager.fetch_trading_calendar(
        TqTradingCalendarRequest(
            start_date=date(2021, 2, 1),
            end_date=date(2021, 2, 3),
        )
    )

    assert [item["date"] for item in result] == [
        "2021-02-01",
        "2021-02-02",
        "2021-02-03",
    ]
    assert all(isinstance(item["trading"], bool) for item in result)
