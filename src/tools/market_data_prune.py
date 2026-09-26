"""一次清理调用；不打开数据库，也不对超时的 POST 重试。"""

from loguru import logger

from src.tools.market_data_collection import JobResult
from src.tools.market_data_http import JobRequestError, MarketDataHttp
from src.tools.market_data_types import RetentionPlan


async def prune_market_data(client: MarketDataHttp, plan: RetentionPlan):
    result = JobResult()
    if not plan.enabled:
        result.skipped = 1
        return result
    body = {
        "providers": plan.providers,
        "modes": plan.modes.model_dump(),
        "auxiliary": plan.auxiliary.model_dump() if "tq" in plan.providers else None,
    }
    try:
        data = await client.request("POST", "/cache/prune", json=body)
        if (
            not isinstance(data, dict)
            or data.get("status") not in {"completed", "partial"}
            or not isinstance(data.get("errors"), list)
            or not isinstance(data.get("auxiliary"), dict)
        ):
            raise JobRequestError("BACKGROUND_INVALID_PRUNE_RESPONSE")
        if data["status"] != "completed" or data["errors"]:
            raise JobRequestError("BACKGROUND_PRUNE_PARTIAL")
        result.succeeded = 1
        logger.bind(ohlcv=data.get("ohlcv"), auxiliary=data["auxiliary"]).info(
            "cache pruning completed"
        )
    except JobRequestError as exc:
        if exc.status == 409:
            result.skipped = 1
            logger.info("cache pruning skipped: busy")
        else:
            result.failure(exc, operation="prune")
    return result
