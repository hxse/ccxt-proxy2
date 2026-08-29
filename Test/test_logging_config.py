from src.tools.logging_config import _sanitize_message, _sanitize_value


def test_sanitize_message_redacts_telegram_bot_token_from_url():
    message = (
        "HTTP Request: POST https://api.telegram.org/"
        'bot123456:ABC-secret-token/sendMessage "HTTP/1.1 200 OK"'
    )

    sanitized = _sanitize_message(message)

    assert "123456:ABC-secret-token" not in sanitized
    assert "https://api.telegram.org/bot***/sendMessage" in sanitized


def test_sanitize_message_redacts_telegram_bot_token_fields():
    message = (
        "bot_token=123456:ABC-secret-token telegram_bot_token:123456:ABC-secret-token"
    )

    sanitized = _sanitize_message(message)

    assert "123456:ABC-secret-token" not in sanitized
    assert "bot_token=***" in sanitized
    assert "telegram_bot_token:***" in sanitized


def test_sanitize_value_redacts_nested_telegram_bot_url():
    sanitized = _sanitize_value(
        {
            "url": "https://api.telegram.org/bot123456:ABC-secret-token/sendMessage",
            "items": ["https://api.telegram.org/bot123456:ABC-secret-token/getMe"],
        }
    )

    assert sanitized == {
        "url": "https://api.telegram.org/bot***/sendMessage",
        "items": ["https://api.telegram.org/bot***/getMe"],
    }


def test_sanitize_value_redacts_telegram_bot_token_fields():
    sanitized = _sanitize_value(
        {
            "bot_token": "123456:ABC-secret-token",
            "telegram_bot_token": "123456:ABC-secret-token",
        }
    )

    assert sanitized == {
        "bot_token": "***",
        "telegram_bot_token": "***",
    }
