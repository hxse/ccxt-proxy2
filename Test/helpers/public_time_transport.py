"""只替换 aiohttp 传输，保留真实 CCXT fetch_time/sign/fetch 调用链。"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx


class TimeSession:
    def __init__(self, handler, calls, *, trust_env, trace_configs):
        self.handler = handler
        self.calls = calls
        self.trust_env = trust_env
        self.trace_configs = trace_configs
        self.headers = {}
        self.closed = False
        for trace in trace_configs:
            trace.freeze()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True

    @asynccontextmanager
    async def get(self, url, **kwargs):
        request = httpx.Request(
            "GET", str(url), headers=kwargs["headers"], content=kwargs["data"]
        )
        request.extensions.update(proxy=kwargs["proxy"], timeout=kwargs["timeout"])
        self.calls.append(request)
        response = self.handler(request)
        if asyncio.iscoroutine(response):
            response = await response

        async def text(**kwargs):
            return response.text

        result = SimpleNamespace(
            status=response.status_code,
            reason=response.reason_phrase,
            headers=response.headers,
            text=text,
        )
        if response.is_redirect:
            for trace in self.trace_configs:
                await trace.on_request_redirect.send(
                    self, SimpleNamespace(), SimpleNamespace(response=result)
                )
        yield result
