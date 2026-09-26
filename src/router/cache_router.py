import asyncio
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool

from src.cache_tool.maintenance_models import MaintenanceFailure
from src.responses_cache import CachePruneResponse, CacheSummaryResponse
from src.router.auth_handler import manager
from src.router.query_validation import reject_unknown_query_params
from src.tools.market_data_dates import years_before
from src.tools.public_time import fetch_public_time
from src.tools.tq_metadata_conversion import natural_date
from src.types_cache import CachePruneRequest, CacheSummaryQuery

cache_router = APIRouter(
    prefix="/cache", dependencies=[Depends(manager)], tags=["Cache"]
)


def _cache(request: Request):
    runtime = request.app.state.service_runtime
    if not runtime.ready:
        raise HTTPException(503, {"code": "CACHE_NOT_READY"})
    try:
        return runtime.cache.get()
    except Exception:
        raise HTTPException(503, {"code": "CACHE_NOT_READY"}) from None


async def _finish_prune(cache, policy, cutoff):
    operation = asyncio.create_task(run_in_threadpool(cache.prune, policy, cutoff))
    cancelled = False
    while not operation.done():
        try:
            await asyncio.shield(operation)
        except asyncio.CancelledError:
            cancelled = True
    result = operation.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


@cache_router.get(
    "/summary",
    response_model=CacheSummaryResponse,
    summary="查看本地连续缓存概况",
    description="只读本地数据库；count 为最新实际片段数量，total_count 为全序列数量。精确身份过滤，不触发 SDK、在线时间或后台配置，不返回隐式保留 K。",
    response_description="items：完整身份、实际首尾、最新连续数量、总量和片段数量。",
    responses={503: {"description": "CACHE_NOT_READY：应用尚未就绪或缓存已关闭。"}},
)
def summary(request: Request, filters: Annotated[CacheSummaryQuery, Query()]):
    return {
        "items": _cache(request).list_series_summaries(
            filters.model_dump(exclude_none=True)
        )
    }


@cache_router.post(
    "/prune",
    response_model=CachePruneResponse,
    summary="[STATEFUL] 清理本地缓存",
    description="会删除本地缓存。显式指定 providers、live/sandbox 保留数量，可选辅助保留年限。每条序列独立事务；0 清空对应普通行情。不会读取后台规则作默认，不压缩整库。",
    response_description="本次规则、删除数量、completed/partial 和错误；HTTP 200 也可能是部分完成。",
    responses={
        409: {"description": "CACHE_MAINTENANCE_BUSY：已有维护，不排队。"},
        500: {
            "description": "CACHE_MAINTENANCE_FAILED，detail.summary 保留已完成部分。"
        },
        503: {"description": "CACHE_NOT_READY：应用未就绪。"},
    },
)
async def prune(request: Request, body: CachePruneRequest):
    reject_unknown_query_params(request, set())
    runtime = request.app.state.service_runtime
    if not runtime.ready:
        raise HTTPException(503, {"code": "CACHE_NOT_READY"})
    if not runtime.cache_maintenance_lock.acquire(blocking=False):
        raise HTTPException(409, {"code": "CACHE_MAINTENANCE_BUSY"})
    try:
        cache = await run_in_threadpool(_cache, request)
        cutoff, clock_error = None, None
        if body.auxiliary is not None:
            try:
                clock = await fetch_public_time()
                cutoff = years_before(
                    natural_date(clock.serverTime), body.auxiliary.keep_years
                )
            except HTTPException as exc:
                detail: Any = exc.detail
                clock_error = (
                    detail.get("code", "PUBLIC_TIME_UNAVAILABLE")
                    if isinstance(detail, dict)
                    else "PUBLIC_TIME_UNAVAILABLE"
                )
            except (ValueError, OverflowError):
                clock_error = "PUBLIC_TIME_INVALID_RESPONSE"
        try:
            result = await _finish_prune(cache, body.policy(), cutoff)
        except MaintenanceFailure as exc:
            raise HTTPException(
                500, {"code": "CACHE_MAINTENANCE_FAILED", "summary": exc.report}
            ) from None
        if clock_error:
            for error in result["errors"]:
                if error["code"] == "TRUSTED_TIME_UNAVAILABLE":
                    error["code"] = clock_error
        return result
    finally:
        runtime.cache_maintenance_lock.release()
