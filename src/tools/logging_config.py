import json
import logging
import os
import re
import sys
from typing import TYPE_CHECKING, Any, cast

from loguru import logger

if TYPE_CHECKING:
    from loguru import Record

SENSITIVE_FIELD_NAMES = {
    "password",
    "secret",
    "api_key",
    "apikey",
    "token",
    "bot_token",
    "telegram_bot_token",
    "access_token",
    "authorization",
    "cookie",
    "set-cookie",
    "signature",
}
MESSAGE_SECRET_PATTERN = re.compile(
    r"(?i)\b(password|secret|signature|api[_-]?key|token|bot[_-]?token|telegram[_-]?bot[_-]?token|access[_-]?token|authorization|cookie)\b(\s*[:=]\s*)([^,\s;&]+)"
)
URL_QUERY_SECRET_PATTERN = re.compile(
    r"(?i)([?&](?:signature|api[_-]?key|apikey|token|access[_-]?token)=)([^&#\s]+)"
)
URL_CREDENTIAL_PATTERN = re.compile(r"://([^:/@\s]+):([^@/\s]+)@")
TELEGRAM_BOT_URL_PATTERN = re.compile(
    r"(https?://api\.telegram\.org/bot)([^/\s\"']+)(/[A-Za-z]+)"
)


def _sanitize_message(message: str) -> str:
    sanitized = MESSAGE_SECRET_PATTERN.sub(r"\1\2***", message)
    sanitized = URL_QUERY_SECRET_PATTERN.sub(r"\1***", sanitized)
    sanitized = URL_CREDENTIAL_PATTERN.sub(r"://***:***@", sanitized)
    return TELEGRAM_BOT_URL_PATTERN.sub(r"\1***\3", sanitized)


def _sanitize_value(value, key: str | None = None):
    if key is not None and key.lower() in SENSITIVE_FIELD_NAMES:
        return "***"

    if isinstance(value, dict):
        return {k: _sanitize_value(v, k) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        sanitized = [_sanitize_value(item) for item in value]
        if isinstance(value, tuple):
            return tuple(sanitized)
        if isinstance(value, set):
            return set(sanitized)
        return sanitized
    if isinstance(value, str):
        return _sanitize_message(value)
    return value


def _format_context(extra: dict) -> str:
    parts: list[str] = []
    for key in sorted(extra):
        if key in {"request_id", "context"}:
            continue

        value = extra[key]
        if value in (None, "", [], {}):
            continue

        if isinstance(value, (dict, list, tuple, set)):
            rendered = json.dumps(value, ensure_ascii=False, default=str)
        else:
            rendered = str(value)
        parts.append(f"{key}={rendered}")

    return " | ".join(parts) if parts else "-"


def _patch_log_record(record: "Record") -> None:
    record["message"] = _sanitize_message(record["message"])
    sanitized_extra = _sanitize_value(record["extra"])
    if not isinstance(sanitized_extra, dict):
        sanitized_extra = {"value": sanitized_extra}
    sanitized_extra.setdefault("request_id", "-")
    sanitized_extra["context"] = _format_context(sanitized_extra)
    record["extra"] = sanitized_extra


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    json_logs = os.getenv("LOG_JSON", "0") == "1"
    enqueue_logs = os.getenv("LOG_ENQUEUE", "0") == "1"

    logger.configure(
        extra={"request_id": "-", "context": ""},
        patcher=cast(Any, _patch_log_record),
    )
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        serialize=json_logs,
        enqueue=enqueue_logs,
        backtrace=False,
        diagnose=False,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
            "{level:<8} | "
            "[{extra[request_id]}] | "
            "{extra[context]} | "
            "{name}:{function}:{line} | "
            "{message}"
        ),
    )

    intercept = InterceptHandler()
    logging.root.handlers = [intercept]
    logging.root.setLevel(level)

    for logger_name in ("uvicorn", "uvicorn.error", "fastapi"):
        std_logger = logging.getLogger(logger_name)
        std_logger.handlers = [intercept]
        std_logger.setLevel(level)
        std_logger.propagate = False

    access_logger = logging.getLogger("uvicorn.access")
    access_logger.handlers = []
    access_logger.propagate = False
    access_logger.disabled = True
