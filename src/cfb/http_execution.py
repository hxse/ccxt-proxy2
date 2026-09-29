"""CFB HTTP 边界只等候 IPC；断开通过 socket EOF 交回唯一执行器处理。"""

import asyncio
from uuid import uuid4

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute

from .client import Client
from .errors import BridgeError, FieldProblem
from .models import Action, Operation, RequestModel
from .terminal.policy import validate_capability


def invalid(location: list[str | int], message: str) -> BridgeError:
    return BridgeError("INVALID_ARGUMENTS", "请求参数无效", 422,
                      details=[FieldProblem(loc=location, type="value_error", message=message)])


async def check_envelope(request: Request) -> None:
    names = list(request.query_params.keys())
    for name in names:
        if len(request.query_params.getlist(name)) != 1:
            raise invalid(["query", name], "单值参数不能重复")
    if request.method == "POST":
        if names:
            raise invalid(["query", names[0]], "POST 参数必须放在 JSON 对象中")
        if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise invalid(["header", "Content-Type"], "需要 application/json")
        if len(request.headers.getlist("idempotency-key")) > 1:
            raise invalid(["header", "Idempotency-Key"], "幂等键不能重复")


class CfbRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handle(request: Request):
            request.state.request_id = "cfb-" + uuid4().hex
            error = None
            try:
                result = await original(request)
            except RequestValidationError as exc:
                problems = [FieldProblem(loc=list(item["loc"]), type=item["type"], message=item["msg"])
                            for item in exc.errors()]
                error = BridgeError("INVALID_ARGUMENTS", "请求参数无效", 422, details=problems)
            except BridgeError as exc:
                error = exc
            except HTTPException:
                raise
            except Exception:
                error = BridgeError("INTERNAL_ERROR", "请求处理异常，核对请求记录", 500,
                    submission_status=getattr(request.state, "submission_status", None))
            if error is not None:
                result = JSONResponse(status_code=error.status,
                    content=error.response(request.state.request_id).model_dump(mode="json"))
            result.headers.setdefault("X-Request-ID", request.state.request_id)
            result.headers["Cache-Control"] = "no-store"
            result.headers["X-Content-Type-Options"] = "nosniff"
            return result

        return handle


async def execute(service: Client, request: Request, action: Action,
                  parameters: RequestModel, key: str | None = None) -> JSONResponse:
    validate_capability(parameters, service.settings)
    operation = Operation(action=action, parameters=parameters.model_dump(mode="json"))
    if action.startswith(("create_", "cancel_")):
        request.state.submission_status = "unknown"
    call = asyncio.create_task(service.call("execute", request_id=request.state.request_id,
                                           operation=operation, key=key))
    try:
        while not call.done():
            await asyncio.wait({call}, timeout=.05)
            if not call.done() and await request.is_disconnected():
                raise BridgeError("REQUEST_CANCELLED", "调用已断开，已开始操作仍由执行器记录", 499,
                                  submission_status=getattr(request.state, "submission_status", None))
        reply = call.result()
        if isinstance(reply.body, dict):
            request.state.request_id = reply.body.get("request_id", request.state.request_id)
            request.state.submission_status = reply.body.get("submission_status")
        return JSONResponse(status_code=reply.status, content=reply.body, headers=reply.headers)
    finally:
        if not call.done():
            call.cancel()
        await asyncio.gather(call, return_exceptions=True)
