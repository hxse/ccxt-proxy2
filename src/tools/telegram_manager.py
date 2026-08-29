import time

import httpx
from fastapi import HTTPException
from loguru import logger

from src.responses_telegram import (
    TelegramSendFailureDetail,
    TelegramSendMessageItem,
    TelegramSendMessageResponse,
)
from src.tools.config_types import TelegramConfig
from src.tools.shared import config
from src.types_telegram import TelegramSendMessageRequest

TELEGRAM_API_BASE_URL = "https://api.telegram.org"
TELEGRAM_HTTP_TIMEOUT_SECONDS = 10.0
TELEGRAM_MAX_RETRIES = 3
TELEGRAM_RETRY_DELAY_SECONDS = 3.0
RETRYABLE_TELEGRAM_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
)


class TelegramManager:
    def __init__(
        self,
        telegram_config: TelegramConfig | None,
        *,
        client: httpx.Client | None = None,
        timeout_seconds: float = TELEGRAM_HTTP_TIMEOUT_SECONDS,
        max_retries: int = TELEGRAM_MAX_RETRIES,
        retry_delay_seconds: float = TELEGRAM_RETRY_DELAY_SECONDS,
        base_url: str = TELEGRAM_API_BASE_URL,
    ):
        self._config = telegram_config
        self._client = client
        self._owns_client = client is None
        self._timeout_seconds = timeout_seconds
        self._max_retries = max(1, max_retries)
        self._retry_delay_seconds = retry_delay_seconds
        self._base_url = base_url.rstrip("/")

    def send_message(
        self, request: TelegramSendMessageRequest
    ) -> TelegramSendMessageResponse:
        telegram_config = self._get_config()
        unknown_chats = [
            chat for chat in request.chats if chat not in telegram_config.chats
        ]
        if unknown_chats:
            raise HTTPException(
                status_code=422,
                detail={
                    "code": "TELEGRAM_UNKNOWN_CHAT",
                    "unknown_chats": unknown_chats,
                },
            )

        items = [
            self._send_message_to_chat(
                telegram_config=telegram_config,
                chat=chat,
                chat_id=telegram_config.chats[chat],
                request=request,
            )
            for chat in request.chats
        ]
        response = TelegramSendMessageResponse(items=items)
        if any(not item.ok for item in response.items):
            detail = TelegramSendFailureDetail(items=response.items)
            raise HTTPException(status_code=502, detail=detail.model_dump())
        return response

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
        self._client = None

    def _get_config(self) -> TelegramConfig:
        if self._config is None:
            raise HTTPException(status_code=500, detail="TELEGRAM_NOT_CONFIGURED")
        return self._config

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self._timeout_seconds)
        return self._client

    def _send_message_to_chat(
        self,
        *,
        telegram_config: TelegramConfig,
        chat: str,
        chat_id: str,
        request: TelegramSendMessageRequest,
    ) -> TelegramSendMessageItem:
        last_result = TelegramSendMessageItem(
            chat=chat,
            chat_id=chat_id,
            ok=False,
            error="telegram send failed",
        )
        for attempt in range(self._max_retries):
            try:
                last_result = self._send_message_once(
                    telegram_config=telegram_config,
                    chat=chat,
                    chat_id=chat_id,
                    request=request,
                )
            except RETRYABLE_TELEGRAM_EXCEPTIONS as exc:
                last_result = TelegramSendMessageItem(
                    chat=chat,
                    chat_id=chat_id,
                    ok=False,
                    error=f"telegram network error: {type(exc).__name__}",
                )
            except httpx.HTTPError as exc:
                logger.bind(chat=chat, error_type=type(exc).__name__).warning(
                    "telegram send_message failed without retry because request may have reached Telegram"
                )
                return TelegramSendMessageItem(
                    chat=chat,
                    chat_id=chat_id,
                    ok=False,
                    error=f"telegram network error: {type(exc).__name__}",
                )

            if last_result.ok:
                if attempt > 0:
                    logger.bind(chat=chat, attempt=attempt + 1).info(
                        "telegram send_message retry succeeded"
                    )
                return last_result
            if not self._is_retryable_result(last_result):
                return last_result
            if attempt < self._max_retries - 1:
                logger.bind(chat=chat, attempt=attempt + 1).warning(
                    "telegram send_message failed before request was delivered, retrying: {}",
                    last_result.error,
                )
                time.sleep(self._retry_delay_seconds)

        return last_result

    def _send_message_once(
        self,
        *,
        telegram_config: TelegramConfig,
        chat: str,
        chat_id: str,
        request: TelegramSendMessageRequest,
    ) -> TelegramSendMessageItem:
        payload = self._build_payload(chat_id, request)
        url = self._send_message_url(telegram_config.bot_token)
        response = self._get_client().post(url, json=payload)

        error = self._response_error(response)
        if error is not None:
            return TelegramSendMessageItem(
                chat=chat,
                chat_id=chat_id,
                ok=False,
                error=error,
            )

        data = response.json()
        result = data.get("result") if isinstance(data, dict) else None
        message_id = result.get("message_id") if isinstance(result, dict) else None
        if not isinstance(message_id, int):
            return TelegramSendMessageItem(
                chat=chat,
                chat_id=chat_id,
                ok=False,
                error="telegram response missing message_id",
            )
        return TelegramSendMessageItem(
            chat=chat,
            chat_id=chat_id,
            ok=True,
            message_id=message_id,
        )

    def _is_retryable_result(self, result: TelegramSendMessageItem) -> bool:
        return bool(
            result.error
            and result.error.startswith("telegram network error:")
            and ("ConnectError" in result.error or "ConnectTimeout" in result.error)
        )

    def _send_message_url(self, bot_token: str) -> str:
        return f"{self._base_url}/bot{bot_token}/sendMessage"

    def _build_payload(
        self, chat_id: str, request: TelegramSendMessageRequest
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "chat_id": chat_id,
            "text": request.text,
            "disable_web_page_preview": request.disable_web_page_preview,
            "disable_notification": request.disable_notification,
        }
        if request.parse_mode is not None:
            payload["parse_mode"] = request.parse_mode
        return payload

    def _response_error(self, response: httpx.Response) -> str | None:
        data: object | None = None
        try:
            data = response.json()
        except ValueError:
            if 200 <= response.status_code < 300:
                return "invalid telegram response"

        if not 200 <= response.status_code < 300:
            return self._telegram_error_description(
                data, fallback=f"telegram http {response.status_code}"
            )

        if not isinstance(data, dict):
            return "invalid telegram response"
        if data.get("ok") is not True:
            return self._telegram_error_description(data, fallback="telegram ok=false")
        return None

    def _telegram_error_description(self, data: object, *, fallback: str) -> str:
        if isinstance(data, dict):
            description = data.get("description")
            if isinstance(description, str) and description:
                return description
        return fallback


telegram_manager = TelegramManager(config.telegram)
