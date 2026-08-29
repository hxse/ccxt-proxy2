import pytest
from pydantic import ValidationError

from src.responses_telegram import (
    TelegramSendMessageItem,
    TelegramSendMessageResponse,
)
from src.router.telegram_router import send_message
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


def test_telegram_route_sends_message(monkeypatch):
    fake_manager = FakeTelegramManager()
    monkeypatch.setattr("src.router.telegram_router.telegram_manager", fake_manager)
    request = TelegramSendMessageRequest(
        chats=["scanner"], text="hello", disable_notification=True
    )
    response = send_message(request)

    assert response.model_dump() == {
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
    monkeypatch.setattr("src.router.telegram_router.telegram_manager", fake_manager)

    with pytest.raises(ValidationError):
        TelegramSendMessageRequest.model_validate(
            {
                "chats": ["scanner"],
                "chat_id": "-1001",
                "text": "hello",
            }
        )

    assert fake_manager.requests == []
