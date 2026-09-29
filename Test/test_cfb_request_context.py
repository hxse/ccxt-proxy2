"""CFB 返回编号贯穿真实 HTTP 中间件；幂等重放保留原编号及本次关联。"""

import pytest
from loguru import logger

from src.cfb.errors import BridgeError
from src.cfb.ipc import Response
from src.cfb.results import PositionsResult, SubmissionResult
from src.router.auth_handler import manager
from src.tools.shared import add_request_context
from Test.test_cfb_http import setup


@pytest.fixture
def completed_logs():
    rows = []
    sink = logger.add(lambda message: rows.append(message.record),
                      filter=lambda record: record["message"] == "request completed")
    try:
        yield rows
    finally:
        logger.remove(sink)


def http_client(monkeypatch, call):
    http = setup(monkeypatch, call)
    http.app.middleware("http")(add_request_context)
    http.app.dependency_overrides[manager] = lambda: {"sub": "offline"}
    return http


@pytest.mark.parametrize("failure", [False, True])
def test_response_and_completion_log_share_request_id(monkeypatch, completed_logs, failure):
    seen = []

    async def call(kind, **kwargs):
        seen.append(kwargs["request_id"])
        if failure:
            raise BridgeError("SERVICE_NOT_READY", "离线通信失败")
        body = PositionsResult(request_id=kwargs["request_id"], positions=[],
                               observed_at="2026-09-29T00:00:00+00:00", source="terminal_csv")
        return Response(status=200, body=body.model_dump(mode="json"),
                        headers={"X-Request-ID": kwargs["request_id"]})

    http = http_client(monkeypatch, call)
    for _ in range(2):
        result = http.get("/cfb/fetch_positions?is_live=false", headers={"X-Request-ID": "caller-repeat"})
        assert result.status_code == (503 if failure else 200)
        identifier = result.headers["X-Request-ID"]
        assert identifier == seen[-1] == completed_logs[-1]["extra"]["request_id"]
        assert identifier.startswith("cfb-") and identifier != "caller-repeat"
        assert result.json()["request_id"] == identifier
        if failure:
            assert result.json()["error"]["code"] == "SERVICE_NOT_READY"
    assert seen[0] != seen[1]


def test_validation_error_uses_same_context_before_ipc(monkeypatch, completed_logs):
    async def forbidden(*args, **kwargs):
        pytest.fail("缺少环境参数不得进入 IPC")

    result = http_client(monkeypatch, forbidden).get("/cfb/fetch_positions")
    assert result.status_code == 422
    assert result.json()["error"]["code"] == "INVALID_ARGUMENTS"
    assert result.json()["request_id"] == result.headers["X-Request-ID"]
    assert completed_logs[-1]["extra"]["request_id"] == result.json()["request_id"]


def test_replay_log_links_saved_identifier_to_current_http_request(monkeypatch, completed_logs):
    payload = SubmissionResult(request_id="cfb-saved", submission_status="submitted",
                               order_id="000123").model_dump(mode="json")
    incoming = []

    async def call(kind, **kwargs):
        incoming.append(kwargs["request_id"])
        return Response(status=202, body=payload, headers={"X-Request-ID": "cfb-saved"})

    result = http_client(monkeypatch, call).post("/cfb/create_limit_order",
        headers={"Idempotency-Key": "saved"}, json={"is_live": False,
        "exchange_id": "DCE", "instrument_id": "m2701", "side": "buy",
        "offset": "open", "volume": 1, "price": 3514})
    assert result.status_code == 202 and result.json() == payload
    assert result.headers["X-Request-ID"] == "cfb-saved"
    extra = completed_logs[-1]["extra"]
    assert extra["request_id"] == "cfb-saved"
    assert extra["http_request_id"] == incoming[0] != "cfb-saved"


def test_other_routes_keep_caller_request_identifier(monkeypatch, completed_logs):
    async def unused(*args, **kwargs):
        pytest.fail("普通路由不应进入 CFB")

    http = http_client(monkeypatch, unused)

    @http.app.get("/ordinary")
    async def ordinary():
        return {"ok": True}

    result = http.get("/ordinary", headers={"X-Request-ID": "caller-normal"})
    assert result.status_code == 200
    assert result.headers["X-Request-ID"] == "caller-normal"
    assert completed_logs[-1]["extra"]["request_id"] == "caller-normal"
    assert "http_request_id" not in completed_logs[-1]["extra"]
