"""按请求转发币安公共时间；不维护时钟、缓存或交易所实例。"""

import asyncio

import httpx
from fastapi import HTTPException
from httpx import AsyncClient

from src.responses_time import PublicTimeResponse

PUBLIC_TIME_URL = "https://api.binance.com/api/v3/time"
PUBLIC_TIME_TIMEOUT_SECONDS = 5.0


async def fetch_public_time() -> PublicTimeResponse:
    try:
        # 请求结束即关闭客户端；不引入后台任务或需要额外初始化的服务。
        async with AsyncClient(timeout=PUBLIC_TIME_TIMEOUT_SECONDS) as client:
            # HTTPX 的分阶段超时之外，再限制整次上游请求的等待时间。
            async with asyncio.timeout(PUBLIC_TIME_TIMEOUT_SECONDS):
                response = await client.get(
                    PUBLIC_TIME_URL, headers={"Cache-Control": "no-cache"}
                )
            response.raise_for_status()
            return PublicTimeResponse.model_validate(response.json())
    except (TimeoutError, httpx.TimeoutException):
        raise HTTPException(504, detail={"code": "PUBLIC_TIME_TIMEOUT"}) from None
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            502,
            detail={
                "code": "PUBLIC_TIME_UPSTREAM_ERROR",
                "upstream_status": exc.response.status_code,
            },
        ) from None
    except httpx.RequestError:
        raise HTTPException(502, detail={"code": "PUBLIC_TIME_NETWORK_ERROR"}) from None
    except ValueError:
        raise HTTPException(
            502, detail={"code": "PUBLIC_TIME_INVALID_RESPONSE"}
        ) from None
