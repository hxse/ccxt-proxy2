import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest
from fastapi import HTTPException

from src.cache_tool import DuckDbOhlcvCache, TqOhlcvBatch, TqOhlcvSeries
from src.responses_tq import (
    TqTradingStatusResponse,
    TqUnderlyingItem,
    TqUnderlyingSymbolResponse,
)
from src.tools.config_types import TqConfig
from src.tools.tq_manager import TqManager
from src.types_tq import TqOhlcvRequest

SYMBOL = "KQ.m@SHFE.rb"
BASE = 1_790_000_000_000_000_001


def records(start=0, count=3):
    return [
        {
            "id": i,
            "datetime": BASE + i * 60_000_000_000,
            "open": 2.0,
            "high": 4.0,
            "low": 1.0,
            "close": 3.0,
            "volume": 10.0,
            "open_oi": None,
            "close_oi": 100.0,
        }
        for i in range(start, start + count)
    ]


class SerialApi:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.rows = records()
        self.calls = []
        self.ready = True
        self.error = None
        self.closed = False

    def create_task(self, coroutine):
        return self.loop.create_task(coroutine)

    def has_trading_status_permission(self):
        return True

    def get_kline_serial(self, symbol, duration, length, adj_type=None):
        assert self.loop.is_running(), "不得进入 SDK 的同步长等待分支"
        self.calls.append((symbol, duration, length, adj_type))
        if self.error:
            raise self.error
        return pd.DataFrame(self.rows, columns=list(records()[0]))

    def get_quote(self, symbol):
        pytest.fail("旧当前标的入口不得调用")

    def query_symbol_info(self, symbol):
        pytest.fail("旧当前标的入口不得调用")

    def is_serial_ready(self, frame):
        return self.ready

    def wait_update(self, deadline):
        self.loop.run_until_complete(asyncio.sleep(0))
        time.sleep(0.001)

    def close(self):
        self.closed = True
        self.loop.close()


@pytest.fixture
def service(tmp_path, monkeypatch):
    cache = DuckDbOhlcvCache(tmp_path / "tq.duckdb", 100_001, 200_000)
    manager = TqManager(TqConfig(), lock_path=tmp_path / "tq.lock")
    created = []

    def create(config):
        api = SerialApi()
        created.append(api)
        return api

    monkeypatch.setattr(manager._client, "_create_api", create)
    manager.initialize(cache)
    try:
        yield manager, created[0], cache
    finally:
        manager.close()
        cache.close()


@pytest.mark.parametrize("count", [1, 5000, 10000, 20000])
def test_one_fixed_sdk_window_and_no_status_on_success(service, monkeypatch, count):
    manager, api, cache = service
    api.rows = records(count=min(3, count))
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda *_: pytest.fail("成功不能查状态"),
    )
    result = asyncio.run(
        manager.fetch_ohlcv(
            TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60, data_length=count)
        )
    )
    assert len(result) == min(3, count)
    assert result[0]["datetime"] == BASE
    assert api.calls == [(SYMBOL, 60, min(count, 10000), None)]
    assert cache.read_latest_summary(TqOhlcvSeries(SYMBOL, 60).key).count == max(
        0, min(3, count) - 1
    )


def test_project_history_extends_sdk_window_to_twenty_thousand(service):
    manager, api, cache = service
    series = TqOhlcvSeries(SYMBOL, 60)
    cache.write_tq_segment(series, TqOhlcvBatch(records(0, 11001)))
    api.rows = records(10000, 10000)
    result = asyncio.run(
        manager.fetch_ohlcv(
            TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60, data_length=20000)
        )
    )
    assert len(result) == 20000
    assert (result[0]["id"], result[-1]["id"]) == (0, 19999)
    assert cache.read_latest_summary(series.key).count == 19999
    assert len(api.calls) == 1


@pytest.mark.parametrize("enabled,duration", [(False, 60), (True, 1209600)])
def test_disabled_or_large_period_never_uses_project_cache(
    service, monkeypatch, enabled, duration
):
    manager, _, cache = service
    monkeypatch.setattr(cache, "write_tq_segment", lambda *_: pytest.fail("不应写缓存"))
    monkeypatch.setattr(
        cache, "read_connected_history", lambda *_: pytest.fail("不应读缓存")
    )
    assert asyncio.run(
        manager.fetch_ohlcv(
            TqOhlcvRequest(
                symbol=SYMBOL, duration_seconds=duration, enable_cache=enabled
            )
        )
    )


def test_gaps_in_clock_are_valid_but_id_gaps_fail(service):
    manager, api, cache = service
    api.rows[1]["datetime"] += 2 * 86400 * 1_000_000_000
    api.rows[2]["datetime"] += 2 * 86400 * 1_000_000_000
    request = TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60)
    assert len(asyncio.run(manager.fetch_ohlcv(request))) == 3
    api.rows[1]["id"] = 5
    with pytest.raises(HTTPException) as error:
        asyncio.run(manager.fetch_ohlcv(request))
    assert (error.value.status_code, error.value.detail) == (
        422,
        "TQ_INVALID_TIME_AXIS",
    )
    assert cache.read_latest_summary(TqOhlcvSeries(SYMBOL, 60).key).count == 2


def test_nullable_prices_return_but_whole_eligible_batch_is_not_stored(service):
    manager, api, cache = service
    api.rows[0]["open"] = None
    assert (
        asyncio.run(
            manager.fetch_ohlcv(TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60))
        )[0]["open"]
        is None
    )
    assert cache.read_latest_summary(TqOhlcvSeries(SYMBOL, 60).key).count == 0
    api.rows[0]["high"] = 0
    with pytest.raises(HTTPException, match="TQ_INVALID_OHLCV_VALUES"):
        asyncio.run(
            manager.fetch_ohlcv(TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60))
        )


@pytest.mark.parametrize("symbol", ["SHFE.rb2610", SYMBOL, "KQ.i@SHFE.rb"])
def test_closed_status_uses_original_series_identity(service, monkeypatch, symbol):
    manager, api, cache = service
    series = TqOhlcvSeries(symbol, 60)
    cache.write_tq_segment(series, TqOhlcvBatch(records()))
    api.error = HTTPException(502, "TQ_NETWORK_UNAVAILABLE")
    checked = []

    def status(actual):
        checked.append(actual)
        return TqTradingStatusResponse(
            symbol=actual, is_open=False, raw_status="NOTRADING"
        )

    monkeypatch.setattr(manager._client.status_snapshot, "read", status)
    resolved = []

    async def mapping(request):
        resolved.append(request.symbol)
        return TqUnderlyingSymbolResponse(
            items=[
                TqUnderlyingItem(symbol=request.symbol, underlying_symbol="SHFE.rb2610")
            ]
        )

    monkeypatch.setattr(manager, "fetch_underlying_symbol", mapping)
    result = asyncio.run(
        manager.fetch_ohlcv(TqOhlcvRequest(symbol=symbol, duration_seconds=60))
    )
    assert [r["id"] for r in result] == [0, 1]
    assert checked == ["SHFE.rb2610"]
    assert resolved == ([] if symbol == "SHFE.rb2610" else [SYMBOL])
    assert all(r["symbol"] == symbol for r in result)


@pytest.mark.parametrize(
    "status,expected",
    [
        ("AUCTIONORDERING", "TQ_NETWORK_UNAVAILABLE"),
        ("CONTINOUS", "TQ_NETWORK_UNAVAILABLE"),
        (None, "TQ_TRADING_STATUS_UNAVAILABLE"),
    ],
)
def test_failed_query_does_not_fallback_on_unknown_or_open(
    service, monkeypatch, status, expected
):
    manager, api, _ = service
    api.error = HTTPException(502, "TQ_NETWORK_UNAVAILABLE")
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda symbol: TqTradingStatusResponse(
            symbol=symbol,
            raw_status=status,
            is_open=None if status is None else status == "CONTINOUS",
        ),
    )
    with pytest.raises(HTTPException, match=expected):
        asyncio.run(
            manager.fetch_ohlcv(
                TqOhlcvRequest(symbol="SHFE.rb2610", duration_seconds=60)
            )
        )


def test_permission_and_invalid_data_do_not_check_status(service, monkeypatch):
    manager, api, _ = service
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda *_: pytest.fail("不可兜底错误不查状态"),
    )
    for error in (
        HTTPException(403, "TQ_PERMISSION_DENIED"),
        HTTPException(422, "TQ_INVALID_TIME_AXIS"),
    ):
        api.error = error
        with pytest.raises(HTTPException) as captured:
            asyncio.run(
                manager.fetch_ohlcv(TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60))
            )
        assert captured.value is error


@pytest.mark.parametrize(
    "failure,code",
    [
        (
            Exception("代码 SHFE.fake 不存在, 请检查合约代码是否填写正确"),
            "TQ_INVALID_SYMBOL",
        ),
        (RuntimeError("unexpected SDK failure"), "TQ_UPSTREAM_ERROR"),
        (ValueError("invalid SDK result"), "TQ_UPSTREAM_ERROR"),
    ],
)
def test_unknown_or_parameter_sdk_failures_cannot_be_hidden_by_closed_cache(
    service, monkeypatch, failure, code
):
    manager, api, cache = service
    cache.write_tq_segment(TqOhlcvSeries(SYMBOL, 60), TqOhlcvBatch(records()))
    api.error = failure
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda *_: pytest.fail("非网络错误不能查询休市兜底"),
    )
    with pytest.raises(HTTPException, match=code):
        asyncio.run(
            manager.fetch_ohlcv(TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60))
        )


def test_raw_serial_deadline_releases_worker_without_late_write(service):
    manager, api, cache = service
    api.ready = False
    request = TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60)
    with pytest.raises(HTTPException, match="TQ_DATA_TIMEOUT"):
        manager.fetch_raw_ohlcv(request, deadline=time.monotonic() + 0.03)
    # 后续工作能被执行，而不是 HTTP 超时后 SDK 线程继续长时间阻塞。
    assert (
        manager._worker.call(lambda: "responsive", deadline=time.monotonic() + 1)
        == "responsive"
    )
    assert cache.read_latest_summary(TqOhlcvSeries(SYMBOL, 60).key).count == 0


def test_close_cancels_serial_wait_and_joins_worker(service):
    manager, api, _ = service
    api.ready = False
    request = TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60)
    with ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(asyncio.run, manager.fetch_ohlcv(request))
        while not api.calls:
            time.sleep(0.001)
        manager.close()
        with pytest.raises(HTTPException, match="TQ_NOT_READY"):
            pending.result(timeout=1)
    assert api.closed and manager._worker._thread is None


def test_unknown_single_tail_cannot_join_old_cache_and_empty_success_is_empty(
    service, monkeypatch
):
    manager, api, cache = service
    series = TqOhlcvSeries(SYMBOL, 60)
    cache.write_tq_segment(series, TqOhlcvBatch(records(0, 4)))
    api.rows = records(2, 1)
    request = TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60, data_length=10)
    assert [r["id"] for r in asyncio.run(manager.fetch_ohlcv(request))] == [2]
    api.rows = []
    monkeypatch.setattr(
        manager._client.status_snapshot,
        "read",
        lambda *_: pytest.fail("空成功不查状态"),
    )
    assert asyncio.run(manager.fetch_ohlcv(request)) == []


def test_full_sdk_window_does_not_read_cache_and_unconnected_window_stays_separate(
    service, monkeypatch
):
    manager, api, cache = service
    series = TqOhlcvSeries(SYMBOL, 60)
    cache.write_tq_segment(series, TqOhlcvBatch(records(0, 4)))
    api.rows = records(10, 3)
    request = TqOhlcvRequest(symbol=SYMBOL, duration_seconds=60, data_length=10)
    assert [r["id"] for r in asyncio.run(manager.fetch_ohlcv(request))] == [10, 11, 12]
    assert cache.read_latest_summary(series.key).segment_count == 2
    monkeypatch.setattr(
        cache, "read_connected_history", lambda *_: pytest.fail("完整 SDK 窗口无需读库")
    )
    assert (
        len(
            asyncio.run(
                manager.fetch_ohlcv(request.model_copy(update={"data_length": 3}))
            )
        )
        == 3
    )
