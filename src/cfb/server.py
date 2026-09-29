"""Unix socket 执行服务；所有交易仍进入原 Dispatcher，不提供 HTTP。"""

import asyncio
import base64
import logging
import threading
import time
from pathlib import Path

from pydantic import JsonValue

from .dispatch import Job
from .errors import BridgeError, FieldProblem
from .ipc import MAX_REQUEST, MAX_RESPONSE, Request, Response, read_frame, write_frame
from .results import Reply
from .service import BridgeService
from .terminal.policy import validate_capability

LOG = logging.getLogger(__name__)


def response(service: BridgeService, reply: Reply) -> Response:
    headers = {"X-Request-ID": reply.request_id, "Cache-Control": "no-store",
               "X-Content-Type-Options": "nosniff"}
    if reply.status == 503 and service.dispatcher is not None:
        retry = service.dispatcher.retry_after()
        if retry is not None:
            headers["Retry-After"] = str(retry)
    return Response(status=reply.status, body=reply.body, headers=headers)


async def execute(service: BridgeService, request: Request, disconnected: asyncio.Task) -> Reply:
    assert request.operation is not None
    operation = request.operation
    validate_capability(operation.request(), service.settings)
    dispatcher = service.dispatcher
    if dispatcher is None:
        raise BridgeError("SERVICE_NOT_READY", "终端执行器尚未启动")
    cancelled = threading.Event()
    admission = asyncio.create_task(asyncio.to_thread(
        dispatcher.submit, request.request_id, operation, request.idempotency_key, cancelled))
    job: Job | None = None
    try:
        while not admission.done():
            await asyncio.wait({admission, disconnected}, return_when=asyncio.FIRST_COMPLETED)
            if disconnected.done():
                cancelled.set()
                # submit 可能已经入队但线程结果尚未送回事件循环，必须收回这个 Job。
                admitted = await asyncio.shield(admission)
                if isinstance(admitted, Job):
                    await asyncio.to_thread(dispatcher.cancel, admitted)
                raise BridgeError("REQUEST_CANCELLED", "通信在准入期间断开，未开始的操作将取消", 499)
        item = admission.result()
        if isinstance(item, Reply):
            return item
        job = item
        future = asyncio.wrap_future(job.future)
        while not future.done():
            await asyncio.wait({future, disconnected}, timeout=.05, return_when=asyncio.FIRST_COMPLETED)
            if disconnected.done():
                cancelled.set()
                removed = await asyncio.to_thread(dispatcher.cancel, job)
                raise BridgeError("REQUEST_CANCELLED", "通信断开；已开始的操作仍由执行器记录结果", 499,
                    submission_status="unknown" if not removed and operation.action.startswith(("create_", "cancel_")) else None)
            if not job.started and time.monotonic() > job.deadline:
                await asyncio.to_thread(dispatcher.cancel, job, timeout=True)
        return future.result()
    except asyncio.CancelledError:
        cancelled.set()
        admission.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
        raise


class Server:
    def __init__(self, service: BridgeService, path: Path):
        self.service = service
        self.path = path
        self.server: asyncio.Server | None = None
        self.owns_socket = False
        self.tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path.unlink(missing_ok=True)  # 调用者已经持有专用数据目录的排他锁。
        self.server = await asyncio.start_unix_server(self._accept, path=self.path, limit=MAX_REQUEST)
        self.owns_socket = True
        self.path.chmod(0o600)

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.owns_socket:
            self.path.unlink(missing_ok=True)
            self.owns_socket = False

    async def handle(self, request: Request, disconnected: asyncio.Task) -> Response:
        if request.kind in {"execute", "ready"}:
            if request.configuration != self.service.settings.identity_signature:
                raise BridgeError("SERVICE_NOT_READY", "CFB 配置与执行器不一致，须完成实例更新")
        if request.kind == "execute":
            reply = await execute(self.service, request, disconnected)
            return response(self.service, reply)
        body: JsonValue
        if request.kind == "health":
            body = {"status": "ok"}
        elif request.kind == "screenshot":
            data = await asyncio.to_thread(self.service.screenshot)
            body = {"content_type": "image/png", "base64": base64.b64encode(data).decode("ascii")}
        else:
            body = await asyncio.to_thread(self.service.control, "status" if request.kind == "ready" else request.kind)
        return Response(status=200, body=body, headers={"Cache-Control": "no-store", "X-Request-ID": request.request_id})

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self.tasks.add(task)
        request = None
        disconnected = None
        try:
            async with asyncio.timeout(5):
                request = Request.model_validate_json(await read_frame(reader, MAX_REQUEST))
            disconnected = asyncio.create_task(reader.read(1))
            try:
                result = await self.handle(request, disconnected)
            except BridgeError as exc:
                result = response(self.service, Reply(request_id=request.request_id, status=exc.status,
                    body=exc.response(request.request_id).model_dump(mode="json")))
            if not disconnected.done():
                try:
                    async with asyncio.timeout(5):
                        await write_frame(writer, result, MAX_RESPONSE)
                except ValueError:
                    error = BridgeError("TERMINAL_DATA_INVALID", "返回结果超过通信容量", 502)
                    await write_frame(writer, response(self.service, Reply(request_id=request.request_id,
                        status=error.status, body=error.response(request.request_id).model_dump(mode="json"))), MAX_RESPONSE)
        except (ValueError, TimeoutError):
            error = BridgeError("INVALID_ARGUMENTS", "内部请求无效或读取超时", 422,
                details=[FieldProblem(loc=["ipc"], type="value_error", message="协议版本、字段或大小不合法")])
            try:
                await write_frame(writer, Response(status=422, body=error.response("cfb-protocol").model_dump(mode="json")), MAX_RESPONSE)
            except (OSError, ValueError):
                pass
        except (ConnectionError, BrokenPipeError):
            pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.error("CFB 通信处理失败：%s", type(exc).__name__)
            if request is not None:
                effect = "unknown" if request.operation and request.operation.action.startswith(("create_", "cancel_")) else None
                error = BridgeError("INTERNAL_ERROR", "执行通信失败，核对请求记录", 500, submission_status=effect)
                try:
                    await write_frame(writer, Response(status=500, body=error.response(request.request_id).model_dump(mode="json")), MAX_RESPONSE)
                except (OSError, ValueError):
                    pass
        finally:
            if disconnected is not None:
                disconnected.cancel()
                await asyncio.gather(disconnected, return_exceptions=True)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.tasks.discard(task)
