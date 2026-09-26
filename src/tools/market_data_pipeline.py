"""顺序执行采集与清理，唯一调度器由应用 lifespan 管理。"""

import asyncio
from time import monotonic

from loguru import logger

from src.tools.config_loader import ConfigError, load_config
from src.tools.market_data_collection import JobResult, collect_market_data
from src.tools.market_data_config import (
    has_work,
    load_market_data_plan,
    tq_enabled,
    validate_client,
)
from src.tools.market_data_http import JobRequestError, MarketDataHttp
from src.tools.market_data_prune import prune_market_data


async def run_once(client, plan, config, scope="both"):
    result = JobResult()
    if scope in {"both", "collect"}:
        result.add(
            await collect_market_data(
                client, plan.tq_collection, enabled=tq_enabled(config)
            )
        )
    if scope in {"both", "prune"}:
        result.add(await prune_market_data(client, plan.retention))
    logger.bind(
        succeeded=result.succeeded, failed=result.failed, skipped=result.skipped
    ).info("market data round finished")
    return result


async def run_cli(scope="both"):
    try:
        config = load_config()
        plan = load_market_data_plan()
        validate_client(plan, config, scope)
    except ConfigError as exc:
        logger.error("{}", exc)
        return 1
    if not has_work(plan, config, scope):
        logger.info("market data round skipped: disabled")
        return 0
    client = MarketDataHttp(config)
    try:
        result = await run_once(client, plan, config, scope)
        return int(result.failed > 0)
    finally:
        await client.close()


class MarketDataScheduler:
    def __init__(self, plan, config):
        self.plan = plan
        self.config = config
        self._task = None

    def start(self):
        if has_work(self.plan, self.config) and self._task is None:
            self._task = asyncio.create_task(self._run(), name="market-data-pipeline")

    async def close(self):
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _run(self):
        client = MarketDataHttp(self.config)
        next_tick = monotonic()
        interval = self.plan.pipeline.interval_seconds
        try:
            while True:
                try:
                    await client.wait_ready()
                    await run_once(client, self.plan, self.config)
                except JobRequestError as exc:
                    logger.bind(error_code=exc.code).warning("market data round failed")
                except Exception as exc:
                    # 未知内部错误也只记录类型，避免异常内容带出账号或 HTTP 请求。
                    logger.bind(exception_type=type(exc).__name__).error(
                        "market data round failed"
                    )
                # 顺序执行；运行期间错过的触发直接跳过，不积压到下轮。
                next_tick += (int((monotonic() - next_tick) // interval) + 1) * interval
                await asyncio.sleep(max(0, next_tick - monotonic()))
        finally:
            await client.close()
