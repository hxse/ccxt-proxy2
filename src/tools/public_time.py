"""通过 CCXT 获取币安 live 时间，复用配置代理且不依赖交易实例。"""

import asyncio
import json

import aiohttp
import ccxt.async_support as ccxt
from fastapi import HTTPException

from src.responses_time import PublicTimeResponse
from src.tools.shared import config

PUBLIC_TIME_TIMEOUT_SECONDS = 5.0


def _upstream_error(status: int) -> HTTPException:
    return HTTPException(
        502,
        detail={"code": "PUBLIC_TIME_UPSTREAM_ERROR", "upstream_status": status},
    )


async def _reject_redirect(session, context, params) -> None:
    # aiohttp 默认跟随重定向；公共取时仍只允许一次固定上游请求。
    raise _upstream_error(params.response.status)


class _TimeBinance(ccxt.binance):
    result: PublicTimeResponse | None = None

    def on_rest_response(
        self,
        code,
        reason,
        url,
        method,
        response_headers,
        response_body,
        request_headers,
        request_body,
    ):
        if not 200 <= code < 300:
            raise _upstream_error(code)
        # SDK 会把字符串/浮点时间转成整数；先验证原值并保留扩展字段。
        self.result = PublicTimeResponse.model_validate(json.loads(response_body))
        return response_body


async def fetch_public_time() -> PublicTimeResponse:
    proxy = (
        config.proxy.effective_http
        if config.binance is not None and config.binance.enable_proxy
        else None
    )
    trace = aiohttp.TraceConfig()
    trace.on_request_redirect.append(_reject_redirect)
    try:
        # 独立异步客户端避免等待交易身份初始化；会话由本次请求完整关闭。
        async with aiohttp.ClientSession(
            trust_env=False, trace_configs=[trace]
        ) as session:
            exchange = _TimeBinance(
                {
                    "session": session,
                    "timeout": int(PUBLIC_TIME_TIMEOUT_SECONDS * 1000),
                    "headers": {"Cache-Control": "no-cache"},
                    "options": {"defaultType": "spot", "maxRetriesOnFailure": 0},
                }
            )
            exchange.aiohttp_trust_env = False
            exchange.aiohttp_proxy = proxy
            async with exchange:
                async with asyncio.timeout(PUBLIC_TIME_TIMEOUT_SECONDS):
                    server_time = await exchange.fetch_time()
                result = exchange.result
                if result is None or server_time != result.serverTime:
                    raise ValueError("invalid exchange time")
                return result
    except (TimeoutError, ccxt.RequestTimeout):
        raise HTTPException(504, detail={"code": "PUBLIC_TIME_TIMEOUT"}) from None
    except ccxt.NetworkError:
        raise HTTPException(502, detail={"code": "PUBLIC_TIME_NETWORK_ERROR"}) from None
    except (ValueError, ccxt.ExchangeError):
        raise HTTPException(
            502, detail={"code": "PUBLIC_TIME_INVALID_RESPONSE"}
        ) from None
