import argparse
import json

from src.tools.telegram_manager import telegram_manager
from src.types_telegram import TelegramSendMessageRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="Telegram send_message probe")
    parser.add_argument("--chat", required=True, help="Configured Telegram chat alias")
    parser.add_argument("--text", required=True, help="Message text")
    parser.add_argument(
        "--parse-mode",
        choices=["MarkdownV2", "HTML"],
        default=None,
        help="Telegram parse_mode",
    )
    parser.add_argument(
        "--disable-notification",
        action="store_true",
        help="Send message silently",
    )
    args = parser.parse_args()

    result = telegram_manager.send_message(
        TelegramSendMessageRequest(
            chats=[args.chat],
            text=args.text,
            parse_mode=args.parse_mode,
            disable_notification=args.disable_notification,
        )
    )
    print(json.dumps(result.model_dump(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
