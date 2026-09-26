"""后台仅通过正式 HTTP 入口工作；令牌不落盘，日志不展开请求/响应。"""

import asyncio
import re
from time import monotonic

import httpx

from src.tools.config_types import AppConfig


class JobRequestError(Exception):
    def __init__(self, code: str, status: int | None = None):
        super().__init__(code)
        self.code = code
        self.status = status


class MarketDataHttp:
    def __init__(self, config: AppConfig, *, transport=None):
        settings = config.market_data_client
        user = settings.user
        if user is None or user not in config.users:
            raise JobRequestError("BACKGROUND_USER_NOT_CONFIGURED")
        self._user = user
        self._password = config.users[user].password
        self._timeout = settings.request_timeout_seconds
        self._http = httpx.AsyncClient(
            base_url=settings.base_url + "/", timeout=self._timeout, transport=transport
        )
        self._token = None
        self._expires = 0.0

    async def close(self):
        self._token = None
        await self._http.aclose()

    async def _send(self, method, path, **kwargs):
        try:
            async with asyncio.timeout(self._timeout):
                return await self._http.request(method, path.lstrip("/"), **kwargs)
        except (TimeoutError, httpx.TimeoutException):
            raise JobRequestError("BACKGROUND_HTTP_TIMEOUT") from None
        except httpx.RequestError:
            raise JobRequestError("BACKGROUND_HTTP_NETWORK_ERROR") from None

    @staticmethod
    def _json(response):
        if not response.is_success:
            code = "BACKGROUND_HTTP_ERROR"
            try:
                detail = response.json().get("detail")
                value = detail.get("code") if isinstance(detail, dict) else None
                if isinstance(value, str) and re.fullmatch(r"[A-Z_]{1,80}", value):
                    code = value
            except (ValueError, AttributeError):
                pass
            raise JobRequestError(code, response.status_code)
        try:
            return response.json()
        except ValueError:
            raise JobRequestError("BACKGROUND_INVALID_RESPONSE") from None

    async def _login(self):
        started = monotonic()
        data = self._json(
            await self._send(
                "POST",
                "/auth/token",
                data={"username": self._user, "password": self._password},
            )
        )
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("access_token"), str)
            or not data["access_token"]
            or str(data.get("token_type", "")).lower() != "bearer"
            or type(data.get("expires_in")) is not int
            or data["expires_in"] <= 0
        ):
            raise JobRequestError("BACKGROUND_INVALID_TOKEN_RESPONSE")
        self._token = data["access_token"]
        # 提前少量刷新，避免令牌恰好在服务器校验时过期。
        lifetime = data["expires_in"]
        self._expires = started + lifetime - min(5.0, lifetime / 10)

    async def request(self, method, path, **kwargs):
        if self._token is None or monotonic() >= self._expires:
            await self._login()
        response = await self._send(
            method, path, headers={"Authorization": f"Bearer {self._token}"}, **kwargs
        )
        if response.status_code == 401:
            # 只有明确未通过鉴权才能重发，超时/5xx 不表明服务端没有执行。
            await self._login()
            response = await self._send(
                method,
                path,
                headers={"Authorization": f"Bearer {self._token}"},
                **kwargs,
            )
        return self._json(response)

    async def wait_ready(self, timeout=60.0):
        try:
            async with asyncio.timeout(timeout):
                while True:
                    try:
                        response = await self._send("GET", "/readyz")
                        if (
                            response.status_code == 200
                            and response.json().get("status") == "ready"
                        ):
                            return
                    except (JobRequestError, ValueError, AttributeError):
                        pass
                    await asyncio.sleep(0.25)
        except TimeoutError:
            raise JobRequestError("BACKGROUND_SERVICE_NOT_READY") from None
