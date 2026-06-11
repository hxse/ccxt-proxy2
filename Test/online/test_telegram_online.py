import os

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.tools.shared import config


pytestmark = pytest.mark.skipif(
    os.getenv("TELEGRAM_ONLINE") != "1",
    reason="Telegram online tests require TELEGRAM_ONLINE=1 and configured Telegram",
)


def test_telegram_online_send_message_route_smoke():
    if config.telegram is None:
        pytest.skip("telegram config is not configured")
    if not config.users:
        pytest.skip("auth users are not configured")

    chat = os.getenv("TELEGRAM_TEST_CHAT") or next(iter(config.telegram.chats))
    text = os.getenv("TELEGRAM_TEST_TEXT") or "ccxt-proxy2 telegram online test"
    username, user = next(iter(config.users.items()))

    with TestClient(app) as client:
        login_response = client.post(
            "/auth/token",
            data={"username": username, "password": user.password},
        )
        assert login_response.status_code == 200
        token = login_response.json()["access_token"]

        response = client.post(
            "/telegram/send_message",
            headers={"Authorization": f"Bearer {token}"},
            json={"chats": [chat], "text": text},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["items"][0]["chat"] == chat
    assert body["items"][0]["ok"] is True
    assert isinstance(body["items"][0]["message_id"], int)
