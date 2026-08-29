import os

import pytest

from src.tools.tq_manager import tq_manager
from src.types_tq import TqOhlcvRequest, TqTickRequest, TqUnderlyingSymbolRequest

pytestmark = pytest.mark.skipif(
    os.getenv("TQ_ONLINE") != "1",
    reason="TQ online tests require TQ_ONLINE=1 and configured TQ credentials",
)


def test_tq_online_fetch_ohlcv_smoke():
    result = tq_manager.fetch_ohlcv(
        TqOhlcvRequest(
            symbol="KQ.m@SHFE.rb",
            duration_seconds=60,
            data_length=10,
        )
    )

    assert isinstance(result, list)


def test_tq_online_fetch_tick_smoke():
    result = tq_manager.fetch_tick(
        TqTickRequest(
            symbol="KQ.m@SHFE.rb",
            data_length=10,
        )
    )

    assert isinstance(result, list)


def test_tq_online_fetch_underlying_symbol_smoke():
    result = tq_manager.fetch_underlying_symbol(
        TqUnderlyingSymbolRequest(symbol="KQ.m@SHFE.rb", n=3)
    )

    assert result.items
