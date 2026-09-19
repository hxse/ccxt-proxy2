from fastapi import APIRouter, Depends, Request, Response

from src.responses_time import PublicTimeErrorResponse, PublicTimeResponse
from src.router.auth_handler import manager
from src.router.query_validation import reject_unknown_query_params
from src.tools.public_time import fetch_public_time

system_router = APIRouter(
    prefix="/system", dependencies=[Depends(manager)], tags=["System"]
)


@system_router.get(
    "/fetch_time",
    response_model=PublicTimeResponse,
    summary="获取公共时间（币安原值转发）",
    description="""
每次请求调用币安公共 `GET https://api.binance.com/api/v3/time`，返回上游原始 JSON 字段。

- 无 query 参数、无请求体；需要本项目的 Bearer token，不需要币安 API key。
- `serverTime` 为整数 Unix 毫秒时间戳，与时区无关；原值转发，不补偿网络延迟。
- 不读取本机时间来生成结果，不缓存时间，不进行后台校时；失败不回退到本机时间。
- 上游请求等待限时 5 秒，无自动重试；异步等待网络，不占用交易/行情 SDK 队列。
- 不创建 CCXT 实例，不需要交易服务白名单；容器只需能够访问上述 HTTPS 接口。
- 502 的 `detail.upstream_status` 可区分上游限流、访问限制等 HTTP 错误。

这是取得上游生成响应时的时间，客户端收到结果时还存在网络传输延迟。
响应带 `Cache-Control: no-store`，避免客户端缓存旧时间。
""",
    response_description="保留币安原始字段；serverTime 为 int64、Unix 毫秒时间戳。",
    responses={
        401: {"description": "未通过本项目 Bearer 鉴权。"},
        422: {"description": "本路由没有 query 参数，传入任何 query 参数都返回 422。"},
        502: {
            "model": PublicTimeErrorResponse,
            "description": "PUBLIC_TIME_NETWORK_ERROR：网络失败；PUBLIC_TIME_UPSTREAM_ERROR：上游 HTTP 错误；PUBLIC_TIME_INVALID_RESPONSE：上游 JSON 或 serverTime 类型不正确。",
        },
        504: {
            "model": PublicTimeErrorResponse,
            "description": "PUBLIC_TIME_TIMEOUT：上游请求超时。",
        },
    },
)
async def fetch_time(request: Request, response: Response) -> PublicTimeResponse:
    """无参数获取币安公共时间，返回原始 serverTime；错误不替换成本机时间。"""
    reject_unknown_query_params(request, set())
    response.headers["Cache-Control"] = "no-store"
    return await fetch_public_time()
