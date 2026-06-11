from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.responses_telegram import (
    TelegramSendMessageItem,
    TelegramSendMessageResponse,
)
from src.router.auth_handler import manager as auth_manager
from src.router.telegram_router import telegram_router
from src.types_telegram import TelegramSendMessageRequest


class FakeTelegramManager:
    def __init__(self):
        self.requests: list[TelegramSendMessageRequest] = []

    def send_message(
        self, request: TelegramSendMessageRequest
    ) -> TelegramSendMessageResponse:
        self.requests.append(request)
        return TelegramSendMessageResponse(
            items=[
                TelegramSendMessageItem(
                    chat=request.chats[0],
                    chat_id="-1001",
                    ok=True,
                    message_id=123,
                )
            ]
        )


def _client(fake_manager: FakeTelegramManager, monkeypatch) -> TestClient:
    app = FastAPI()
    app.dependency_overrides[auth_manager] = lambda: "test-user"
    monkeypatch.setattr(
        "src.router.telegram_router.telegram_manager",
        fake_manager,
    )
    app.include_router(telegram_router)
    return TestClient(app)


def test_telegram_route_sends_message(monkeypatch):
    fake_manager = FakeTelegramManager()
    client = _client(fake_manager, monkeypatch)

    response = client.post(
        "/telegram/send_message",
        json={
            "chats": ["scanner"],
            "text": "hello",
            "disable_notification": True,
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "chat": "scanner",
                "chat_id": "-1001",
                "ok": True,
                "message_id": 123,
                "error": None,
            }
        ]
    }
    assert fake_manager.requests == [
        TelegramSendMessageRequest(
            chats=["scanner"],
            text="hello",
            disable_notification=True,
        )
    ]


def test_telegram_route_rejects_raw_chat_id(monkeypatch):
    fake_manager = FakeTelegramManager()
    client = _client(fake_manager, monkeypatch)

    response = client.post(
        "/telegram/send_message",
        json={
            "chats": ["scanner"],
            "chat_id": "-1001",
            "text": "hello",
        },
    )

    assert response.status_code == 422
    assert fake_manager.requests == []
