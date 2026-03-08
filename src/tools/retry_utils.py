import ssl
import time
from collections.abc import Callable
from typing import Any, TypeVar

import ccxt
from loguru import logger

DEFAULT_NETWORK_RETRY_COUNT = 1
DEFAULT_NETWORK_RETRY_DELAY_SECONDS = 0.5

T = TypeVar("T")

NETWORK_RETRY_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ccxt.NetworkError,
    ssl.SSLError,
    TimeoutError,
)

try:
    import requests

    NETWORK_RETRY_EXCEPTIONS = NETWORK_RETRY_EXCEPTIONS + (
        requests.exceptions.RequestException,
    )
except ImportError:
    pass


def call_with_retry(
    func: Callable[..., T],
    *args: Any,
    retries: int = DEFAULT_NETWORK_RETRY_COUNT,
    retry_delay_seconds: float = DEFAULT_NETWORK_RETRY_DELAY_SECONDS,
    operation_name: str | None = None,
    **kwargs: Any,
) -> T:
    retry_count = max(retries, 0)
    total_attempts = retry_count + 1
    op_name = operation_name or getattr(func, "__name__", "operation")

    for attempt in range(1, total_attempts + 1):
        try:
            return func(*args, **kwargs)
        except NETWORK_RETRY_EXCEPTIONS as exc:
            if attempt >= total_attempts:
                raise

            logger.bind(
                operation=op_name,
                attempt=attempt,
                retries=retry_count,
                retry_delay_seconds=retry_delay_seconds,
            ).warning(
                "network operation failed, retrying after {}s: {}",
                retry_delay_seconds,
                exc,
            )
            time.sleep(retry_delay_seconds)

    raise RuntimeError(f"retry loop for {op_name} exited unexpectedly")
