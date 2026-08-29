import os

import pytest

from src.tools.shared import config
from src.tools.telegram_manager import telegram_manager
from src.types_telegram import TelegramSendMessageRequest

pytestmark = pytest.mark.skipif(
    os.getenv("TELEGRAM_ONLINE") != "1",
    reason="Telegram online tests require TELEGRAM_ONLINE=1 and configured Telegram",
)


def test_telegram_online_send_message_smoke():
    if config.telegram is None:
        pytest.skip("telegram config is not configured")

    chat = os.getenv("TELEGRAM_TEST_CHAT") or next(iter(config.telegram.chats))
    text = os.getenv("TELEGRAM_TEST_TEXT") or "ccxt-proxy2 telegram online test"

    response = telegram_manager.send_message(
        TelegramSendMessageRequest(chats=[chat], text=text)
    )

    assert response.items[0].chat == chat
    assert response.items[0].ok is True
    assert isinstance(response.items[0].message_id, int)
