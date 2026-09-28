"""官方静态源的有界下载，只共享在途请求，不将旧结果当作刷新。"""

import asyncio
import os

import httpx
from fastapi import HTTPException

from src.tools.tq_metadata_conversion import parse_holidays, parse_mapping


class MetadataSource:
    def __init__(self, *, proxy_url: str | None = None):
        self._proxy_url = proxy_url
        self.urls = {
            "calendar": os.getenv(
                "TQ_CHINESE_HOLIDAY_URL",
                "https://files.shinnytech.com/shinny_chinese_holiday.json",
            ),
            "mapping": os.getenv(
                "TQ_CONT_TABLE_URL",
                "https://files.shinnytech.com/continuous_table.json",
            ),
        }
        self._client: httpx.AsyncClient | None = None
        self._pending: dict[str, asyncio.Task] = {}
        self._closed = False

    async def fetch(self, kind: str, headers: dict[str, str]):
        if self._closed:
            raise HTTPException(503, "TQ_NOT_READY")
        task = self._pending.get(kind)
        if task is None or task.done():
            task = asyncio.create_task(self._download(kind, headers))
            self._pending[kind] = task
        try:
            return await asyncio.shield(task)
        finally:
            if task.done() and self._pending.get(kind) is task:
                self._pending.pop(kind, None)

    async def _download(self, kind: str, headers: dict[str, str]):
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=10,
                follow_redirects=True,
                proxy=self._proxy_url,
                trust_env=False,
            )
        try:
            async with asyncio.timeout(10):
                response = await self._client.get(
                    self.urls[kind], headers=dict(headers)
                )
                response.raise_for_status()
                raw = response.json()
                return parse_holidays(raw) if kind == "calendar" else parse_mapping(raw)
        except (TimeoutError, httpx.TimeoutException):
            raise HTTPException(504, "TQ_METADATA_SOURCE_TIMEOUT") from None
        except httpx.HTTPError:
            raise HTTPException(502, "TQ_METADATA_SOURCE_UNAVAILABLE") from None
        except ValueError:
            raise HTTPException(502, "TQ_METADATA_INVALID_SOURCE") from None

    async def close(self):
        self._closed = True
        tasks = list(self._pending.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._pending.clear()
        if self._client is not None:
            await self._client.aclose()
