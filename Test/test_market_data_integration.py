import asyncio
from datetime import date

import httpx

from src.cache_tool import DuckDbOhlcvCache, OhlcvResult, OhlcvSeries, TqOhlcvSeries
from src.cache_tool.maintenance_models import RetentionPolicy
from src.cache_tool.transition_store import TransitionIdentity
from src.main import app
from src.tools.market_data_collection import collect_market_data
from src.tools.market_data_http import MarketDataHttp
from src.tools.market_data_types import TqCollectionPlan
from src.tools.service_runtime import ServiceRuntime
from src.tools.shared import lifespan
from src.tools.tq_manager import TqManager
from Test.test_market_data_config import job_config
from Test.test_tq_metadata_cache import calendar, mapping
from Test.test_tq_ohlcv_cache import SYMBOL, SerialApi, records
from Test.test_tq_transition_cache import batch


def test_user_background_prune_and_restart_share_one_http_cache_chain(
    tmp_path, monkeypatch
):
    config = job_config()
    config.ohlcv_cache.database_path = str(tmp_path / "integration.duckdb")
    runtime = ServiceRuntime(config)
    manager = TqManager(
        config.tq, tmp_path / "integration.lock", access_guard=runtime.require
    )
    created = []

    def factory(_):
        api = SerialApi()
        created.append(api)
        return api

    monkeypatch.setattr(manager._client, "_create_api", factory)
    monkeypatch.setattr("src.tools.shared.service_runtime", runtime)
    monkeypatch.setattr("src.tools.tq_manager.tq_manager", manager)
    monkeypatch.setattr("src.router.tq_router.tq_manager", manager)
    monkeypatch.setattr("src.router.auth_handler.config", config)
    series = TqOhlcvSeries(SYMBOL, 60)

    async def run():
        client = MarketDataHttp(config, transport=httpx.ASGITransport(app=app))
        try:
            async with lifespan(app):
                await asyncio.to_thread(runtime.wait_for_startup)
                created[-1].rows = records(0, 5)
                request = {"symbol": SYMBOL, "duration_seconds": 60, "data_length": 10}
                user = await client.request("GET", "/tq/fetch_ohlcv", params=request)
                assert [row["id"] for row in user] == list(range(5))
                assert runtime.cache.get().read_latest_summary(series.key).count == 4
                # 后台 5m 是独立序列；同一 HTTP 链留下真实持久化前缀。
                created[-1].rows = records(2, 5)
                plan = TqCollectionPlan(
                    symbols={"rb": SYMBOL},
                    timeframes=["5m"],
                    save_weighted=False,
                    save_mapping=False,
                    save_transition=False,
                    save_calendar=False,
                )
                result = await collect_market_data(client, plan)
                assert result.failed == 0 and result.succeeded == 1
                main5 = TqOhlcvSeries(SYMBOL, 300)
                assert runtime.cache.get().read_latest_summary(main5.key).count == 4
                # 用户在同一后台序列取更大的数量，自然通过实际重叠续接。
                created[-1].rows = records(4, 5)
                user5 = await client.request(
                    "GET", "/tq/fetch_ohlcv", params=request | {"duration_seconds": 300}
                )
                assert [row["id"] for row in user5] == list(range(2, 9))
                pruned = await client.request(
                    "POST",
                    "/cache/prune",
                    json={"providers": ["tq"], "modes": {"live": 3, "sandbox": 0}},
                )
                assert pruned["status"] == "completed"
                summaries = await client.request(
                    "GET", "/cache/summary", params={"is_live": True, "symbol": SYMBOL}
                )
                assert {item["total_count"] for item in summaries["items"]} == {3}
                assert len(user5) == 7  # 清理不能追溯缩短已经成功的响应。
                old_cache = runtime.cache.get()
            assert old_cache._closed and created[-1].closed
            async with lifespan(app):
                await asyncio.to_thread(runtime.wait_for_startup)
                assert runtime.cache.get() is not old_cache
                confirmed = (
                    runtime.cache.get()
                    .read_contiguous_before(main5.key, 2**63 - 1, 10)
                    .rows
                )
                assert [row["id"] for row in confirmed] == [5, 6, 7]
        finally:
            await client.close()

    asyncio.run(run())


def test_ordinary_capacity_prune_and_reopen_preserve_auxiliary_proofs(tmp_path):
    path = tmp_path / "all-kinds.duckdb"
    cache = DuckDbOhlcvCache(path, 100_001, 200_000)
    mapped = mapping()
    identity = TransitionIdentity(
        mapped.symbol,
        mapped.nodes[0].date,
        mapped.nodes[0].underlying_symbol,
        mapped.nodes[0].old_symbol,
        300,
    )
    cache.submit_calendar(calendar(1, 3))
    cache.submit_mapping(mapped)
    cache.submit_transition_window(identity, 3, batch(3))
    cache.max_rows_total = 3
    key = OhlcvSeries("binance", "live", "future", "BTC/USDT:USDT", "1m").key
    cache.write_segment(
        key, OhlcvResult([(i, 1.0, 2.0, 0.0, 1.0, 3.0) for i in range(1, 6)], True), 1
    )
    cache.prune(RetentionPolicy(("ccxt",), 1, 0))
    cache.close()
    reopened = DuckDbOhlcvCache(path, 100_001, 200_000)
    try:
        assert (
            reopened.read_calendar_range(date(2026, 9, 1), date(2026, 9, 3)) is not None
        )
        history = reopened.read_mapping_range(
            mapped.symbol, [r.date for r in mapped.records], mapped.facts.verified_date
        )
        assert history is not None and history.nodes == mapped.nodes
        assert len(reopened.read_transition_prefix(identity, 3)) == 3
        assert reopened.read_latest_summary(key).count == 1
        assert reopened._connection().execute(
            "SELECT value FROM cache_meta WHERE key='schema_version'"
        ).fetchone() == ("5",)
    finally:
        reopened.close()
