import json
from typing import Any, cast

import httpx
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from src.responses_telegram import TelegramSendMessageItem
from src.tools.config_types import TelegramConfig
from src.tools.telegram_manager import TelegramManager
from src.types_telegram import TelegramSendMessageRequest


def _manager(
    handler,
    *,
    bot_token: str = "secret-token",
    chats: dict[str, str] | None = None,
    max_retries: int = 1,
) -> TelegramManager:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return TelegramManager(
        TelegramConfig(
            bot_token=bot_token,
            chats=chats or {"scanner": "-1001", "ops": "-1002"},
        ),
        client=client,
        max_retries=max_retries,
        retry_delay_seconds=0.0,
    )


def _detail(exc: HTTPException) -> dict[str, Any]:
    return cast(dict[str, Any], exc.detail)


def test_telegram_config_requires_named_chats():
    with pytest.raises(ValidationError):
        TelegramConfig(bot_token="token", chats={})

    with pytest.raises(ValidationError):
        TelegramConfig(bot_token="token", chats={"bad alias": "-1001"})

    with pytest.raises(ValidationError):
        TelegramConfig(bot_token="token", chats={"scanner": " "})


def test_telegram_request_rejects_invalid_targets_and_blank_text():
    with pytest.raises(ValidationError):
        TelegramSendMessageRequest(chats=[""], text="hello")

    with pytest.raises(ValidationError):
        TelegramSendMessageRequest(chats=["scanner", " scanner "], text="hello")

    with pytest.raises(ValidationError):
        TelegramSendMessageRequest(chats=["scanner"], text="   ")

    with pytest.raises(ValidationError):
        TelegramSendMessageRequest.model_validate(
            {
                "chats": ["scanner"],
                "text": "hello",
                "chat_id": "-1001",
            }
        )


def test_telegram_response_item_state_is_strict():
    TelegramSendMessageItem(
        chat="scanner",
        chat_id="-1001",
        ok=True,
        message_id=1,
    )
    TelegramSendMessageItem(
        chat="scanner",
        chat_id="-1001",
        ok=False,
        error="failed",
    )

    with pytest.raises(ValidationError):
        TelegramSendMessageItem(
            chat="scanner",
            chat_id="-1001",
            ok=True,
            message_id=1,
            error="should-not-exist",
        )

    with pytest.raises(ValidationError):
        TelegramSendMessageItem(
            chat="scanner",
            chat_id="-1001",
            ok=False,
            message_id=1,
            error="failed",
        )


def test_telegram_send_message_supports_multiple_chats():
    requests: list[dict[str, object]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={"ok": True, "result": {"message_id": len(requests)}},
        )

    manager = _manager(handler)

    result = manager.send_message(
        TelegramSendMessageRequest(
            chats=["scanner", "ops"],
            text="hello",
            parse_mode="HTML",
            disable_web_page_preview=False,
            disable_notification=True,
        )
    )

    assert [item.chat for item in result.items] == ["scanner", "ops"]
    assert [item.message_id for item in result.items] == [1, 2]
    assert requests == [
        {
            "chat_id": "-1001",
            "text": "hello",
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
            "disable_notification": True,
        },
        {
            "chat_id": "-1002",
            "text": "hello",
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
            "disable_notification": True,
        },
    ]


def test_telegram_unknown_chat_returns_422_without_http_call():
    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, json={"ok": True})

    manager = _manager(handler)

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["unknown"], text="hello")
        )

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == {
        "code": "TELEGRAM_UNKNOWN_CHAT",
        "unknown_chats": ["unknown"],
    }
    assert called is False


def test_telegram_partial_failure_returns_502_with_all_results():
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        if body["chat_id"] == "-1002":
            return httpx.Response(
                400,
                json={"ok": False, "description": "Bad Request: chat not found"},
            )
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 10}})

    manager = _manager(handler)

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["scanner", "ops"], text="hello")
        )

    assert exc_info.value.status_code == 502
    detail = _detail(exc_info.value)
    assert detail["code"] == "TELEGRAM_SEND_FAILED"
    assert detail["items"] == [
        {
            "chat": "scanner",
            "chat_id": "-1001",
            "ok": True,
            "message_id": 10,
            "error": None,
        },
        {
            "chat": "ops",
            "chat_id": "-1002",
            "ok": False,
            "message_id": None,
            "error": "Bad Request: chat not found",
        },
    ]


def test_telegram_ok_false_and_non_json_are_failures_without_token_leak():
    def ok_false_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ok": False, "description": "Bad Request: blocked"},
        )

    manager = _manager(ok_false_handler, bot_token="very-secret-token")

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["scanner"], text="hello")
        )

    assert exc_info.value.status_code == 502
    assert "very-secret-token" not in str(exc_info.value.detail)
    detail = _detail(exc_info.value)
    assert detail["items"][0]["error"] == "Bad Request: blocked"

    def non_json_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    manager = _manager(non_json_handler)

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["scanner"], text="hello")
        )

    assert exc_info.value.status_code == 502
    detail = _detail(exc_info.value)
    assert detail["items"][0]["error"] == "invalid telegram response"


def test_telegram_success_response_requires_message_id():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True, "result": {}})

    manager = _manager(handler)

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["scanner"], text="hello")
        )

    assert exc_info.value.status_code == 502
    detail = _detail(exc_info.value)
    assert detail["items"][0]["error"] == "telegram response missing message_id"


def test_telegram_retries_connect_errors_only():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("connect failed", request=request)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 2}})

    manager = _manager(handler, max_retries=2)

    result = manager.send_message(
        TelegramSendMessageRequest(chats=["scanner"], text="hello")
    )

    assert attempts == 2
    assert result.items[0].message_id == 2


def test_telegram_does_not_retry_read_timeout_to_avoid_duplicate_delivery():
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("read timed out", request=request)

    manager = _manager(handler, max_retries=3)

    with pytest.raises(HTTPException) as exc_info:
        manager.send_message(
            TelegramSendMessageRequest(chats=["scanner"], text="hello")
        )

    assert attempts == 1
    assert exc_info.value.status_code == 502
    detail = _detail(exc_info.value)
    assert detail["items"][0]["error"] == "telegram network error: ReadTimeout"
