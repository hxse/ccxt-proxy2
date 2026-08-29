import ssl
import threading
import time
from typing import Any

import ccxt
from loguru import logger

from src.domain_errors import CapabilityNotSupported, NetworkIncomplete

NETWORK_RETRY_EXCEPTIONS: tuple[type[BaseException], ...] = (
    ccxt.NetworkError,
    ssl.SSLError,
    TimeoutError,
)


class CcxtTransport:
    """Private per-instance lock and retry boundary used only by CcxtClient."""

    def __init__(self, exchange: Any, identity: str) -> None:
        self.exchange = exchange
        self.identity = identity
        self.lock = threading.Lock()

    def require(self, capability: str) -> None:
        if not self.exchange.has.get(capability):
            raise CapabilityNotSupported(
                f"{self.identity} does not support {capability}"
            )

    def read_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        self.require(capability)
        return self.read_call(
            capability, getattr(self.exchange, method), *args, **kwargs
        )

    def write_method(self, capability: str, method: str, *args: Any, **kwargs: Any):
        self.require(capability)
        return self.write_call(
            capability, getattr(self.exchange, method), *args, **kwargs
        )

    def read_call(self, operation: str, function, *args: Any, **kwargs: Any):
        for attempt in range(2):
            try:
                return self._attempt(function, *args, **kwargs)
            except NETWORK_RETRY_EXCEPTIONS as exc:
                if attempt == 1:
                    raise NetworkIncomplete(
                        f"{self.identity} {operation} failed after retry"
                    ) from exc
                logger.bind(operation=f"{self.identity} {operation}").warning(
                    "network operation failed; retrying after 0.5s: {}", exc
                )
                time.sleep(0.5)
        raise RuntimeError("unreachable retry state")

    def write_call(self, operation: str, function, *args: Any, **kwargs: Any):
        try:
            return self._attempt(function, *args, **kwargs)
        except NETWORK_RETRY_EXCEPTIONS:
            logger.bind(operation=operation).exception(
                "non-read operation failed; operation status may be unknown"
            )
            raise

    def _attempt(self, function, *args: Any, **kwargs: Any):
        with self.lock:
            return function(*args, **kwargs)
