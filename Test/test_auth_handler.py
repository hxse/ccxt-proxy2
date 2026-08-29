import asyncio

import httpx
from fastapi import FastAPI, Response
from fastapi.security import OAuth2PasswordRequestForm

from src.router import auth_handler
from src.tools.config_types import UserConfig


def _post(app: FastAPI, path: str, data: dict[str, str]) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.post(path, data=data)

    return asyncio.run(request())


def test_login_reports_access_token_expiry(monkeypatch):
    username = "bruno-test"
    password = "secret"
    captured = {}

    def create_access_token(*, data, expires):
        captured["data"] = data
        captured["expires"] = expires
        return "test-token"

    monkeypatch.setitem(
        auth_handler.config.users,
        username,
        UserConfig(password=password),
    )
    monkeypatch.setattr(auth_handler.manager, "create_access_token", create_access_token)
    monkeypatch.setattr(auth_handler.manager, "set_cookie", lambda response, token: None)
    form = OAuth2PasswordRequestForm(
        grant_type="password",
        username=username,
        password=password,
    )

    result = auth_handler.login(Response(), form)

    assert result["access_token"] == "test-token"
    assert result["token_type"] == "bearer"
    assert result["expires_in"] == 3600
    assert captured["data"] == {"sub": username}
    assert captured["expires"].total_seconds() == 3600


def test_password_grant_http_response_supports_bruno_auto_fetch(monkeypatch):
    monkeypatch.setitem(
        auth_handler.config.users,
        "bruno-http",
        UserConfig(password="secret"),
    )
    monkeypatch.setattr(
        auth_handler.manager,
        "create_access_token",
        lambda **kwargs: "http-token",
    )
    app = FastAPI()
    app.include_router(auth_handler.auth_router)

    response = _post(
        app,
        "/auth/token",
        {
            "grant_type": "password",
            "username": "bruno-http",
            "password": "secret",
            "client_id": "_",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "access_token": "http-token",
        "token_type": "bearer",
        "expires_in": 3600,
    }


def test_invalid_password_is_rejected_and_refresh_route_is_not_exposed(monkeypatch):
    monkeypatch.setitem(
        auth_handler.config.users,
        "bruno-http",
        UserConfig(password="secret"),
    )
    app = FastAPI()
    app.include_router(auth_handler.auth_router)

    invalid = _post(
        app,
        "/auth/token",
        {"username": "bruno-http", "password": "wrong"},
    )
    refresh = _post(
        app,
        "/auth/refresh",
        {"grant_type": "refresh_token", "refresh_token": "unused"},
    )

    assert invalid.status_code == 401
    assert refresh.status_code == 404
