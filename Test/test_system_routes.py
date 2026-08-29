import json

from fastapi.responses import JSONResponse

from src.main import healthz, readyz
from src.responses_system import HealthResponse, NotReadyResponse
from src.tools.shared import app


def test_health_response_has_a_stable_schema():
    assert HealthResponse.model_validate(healthz()).status == "ok"


def test_not_ready_response_never_exposes_startup_exception(monkeypatch):
    monkeypatch.setattr(app.state, "exchange_registry_ready", False)
    monkeypatch.setattr(app.state, "exchange_registry_initialized", [])

    response = readyz()

    assert isinstance(response, JSONResponse)
    body = json.loads(response.body)
    assert NotReadyResponse.model_validate(body).status == "not_ready"
    assert "error" not in body
