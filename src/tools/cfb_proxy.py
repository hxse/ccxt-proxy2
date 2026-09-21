"""CFB HTTP 薄转发：复用连接，只限制等待时间，不解析业务参数或重试。"""

import asyncio

import httpx
from fastapi import HTTPException, Request, Response
from httpx import AsyncClient

from src.tools.config_types import CfbConfig

REQUEST_HEADERS = {
    b"accept",
    b"content-type",
    b"content-encoding",
    b"idempotency-key",
    b"x-request-id",
}
HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}


class CfbProxy:
    def __init__(self) -> None:
        self._client: AsyncClient | None = None
        self._timeout = 300.0

    def initialize(self, config: CfbConfig) -> None:
        """启动阶段创建客户端；CFB 容器的启动和登录由上游负责。"""
        if self._client is None:
            self._timeout = config.request_timeout_seconds
            self._client = AsyncClient(
                base_url=str(config.base_url).rstrip("/") + "/",
                timeout=self._timeout,
                follow_redirects=False,
                trust_env=False,
            )

    async def close(self) -> None:
        """在应用事件循环中释放 HTTP 连接。"""
        client, self._client = self._client, None
        if client is not None:
            await client.aclose()

    async def forward(self, request: Request, path: str) -> Response:
        client = self._client
        if client is None:
            raise HTTPException(
                503, detail={"code": "SERVICE_NOT_READY", "service": "cfb"}
            )
        url = client.base_url.join(path.lstrip("/")).copy_with(
            query=request.scope["query_string"]
        )
        upstream_request = client.build_request(
            request.method,
            url,
            content=await request.body(),
            headers=[
                (key, value)
                for key, value in request.headers.raw
                if key.lower() in REQUEST_HEADERS
            ],
        )
        # 每次仅转发调用方的请求，不向 CFB 传递本项目 JWT 或客户端 Cookie。
        upstream_request.headers.pop("cookie", None)
        try:
            async with asyncio.timeout(self._timeout):
                upstream = await client.send(upstream_request)
        except (TimeoutError, httpx.TimeoutException):
            raise HTTPException(504, detail={"code": "CFB_PROXY_TIMEOUT"}) from None
        except httpx.RequestError:
            raise HTTPException(
                502, detail={"code": "CFB_PROXY_NETWORK_ERROR"}
            ) from None

        # HTTPX 已解压正文；重算长度，保留上游业务头和所有响应字段。
        excluded = HOP_HEADERS | {b"content-length", b"content-encoding"}
        excluded |= {
            name.strip().lower().encode("ascii")
            for name in upstream.headers.get("connection", "").split(",")
            if name.strip()
        }
        response = Response(content=upstream.content, status_code=upstream.status_code)
        response.raw_headers.extend(
            (key.lower(), value)
            for key, value in upstream.headers.raw
            if key.lower() not in excluded
        )
        return response


cfb_proxy = CfbProxy()
