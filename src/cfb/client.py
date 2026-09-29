"""主服务的轻量 CFB 客户端；每次请求只连接 Unix socket，不管理容器。"""

import asyncio
from pathlib import Path
from uuid import uuid4

from .config import Settings
from .errors import BridgeError
from .ipc import (
    MAX_REQUEST,
    MAX_RESPONSE,
    Kind,
    Request,
    Response,
    read_frame,
    write_frame,
)
from .models import Operation


class Client:
    def __init__(self, path: Path):
        self.path = path
        self.timeout = 300.0
        self.enabled = False
        self.settings = Settings()
        self.writers: set[asyncio.StreamWriter] = set()

    def initialize(self, config) -> None:
        self.timeout = config.request_timeout_seconds
        self.settings = config.model_copy(deep=True)
        self.enabled = True

    def is_ready(self) -> bool:
        # 客户端就绪与终端登录状态分开；不可在此拦截已有幂等响应重放。
        return self.enabled

    async def close(self) -> None:
        self.enabled = False
        for writer in tuple(self.writers):
            writer.close()
        await asyncio.gather(*(writer.wait_closed() for writer in tuple(self.writers)), return_exceptions=True)

    async def call(self, kind: Kind, *, request_id: str | None = None,
                   operation: Operation | None = None, key: str | None = None) -> Response:
        if not self.enabled:
            raise BridgeError("SERVICE_NOT_READY", "CFB 客户端未就绪")
        request = Request(kind=kind, request_id=request_id or "cfb-" + uuid4().hex,
                          operation=operation, idempotency_key=key,
                          configuration=self.settings.identity_signature)
        writer = None
        sent = False
        try:
            async with asyncio.timeout(self.timeout):
                reader, writer = await asyncio.open_unix_connection(str(self.path), limit=MAX_RESPONSE)
                self.writers.add(writer)
                sent = True
                await write_frame(writer, request, MAX_REQUEST)
                result = Response.model_validate_json(await read_frame(reader, MAX_RESPONSE))
                if not set(result.headers) <= {"X-Request-ID", "Cache-Control", "X-Content-Type-Options", "Retry-After"}:
                    raise ValueError("unexpected response headers")
                return result
        except (OSError, TimeoutError, ValueError):
            effect = "unknown" if sent and operation and operation.action.startswith(("create_", "cancel_")) else None
            raise BridgeError("SERVICE_NOT_READY", "CFB 通信不可用；已发送操作须核对结果", 503,
                              submission_status=effect) from None
        finally:
            if writer is not None:
                self.writers.discard(writer)
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
