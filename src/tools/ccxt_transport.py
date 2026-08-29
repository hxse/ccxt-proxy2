import ssl
import threading
import time
from typing import Any

import ccxt
from loguru import logger

from src.domain_errors import (
    CapabilityNotSupported,
    NetworkIncomplete,
    OperationStatusUnknown,
    ProviderClientClosed,
)

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
        self._closed = False

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
        except NETWORK_RETRY_EXCEPTIONS as exc:
            logger.bind(operation=operation).exception(
                "non-read operation failed; operation status may be unknown"
            )
            raise OperationStatusUnknown(
                f"{self.identity} {operation} status is unknown"
            ) from exc

    def close(self) -> None:
        with self.lock:
            if self._closed:
                return
            self._closed = True
            close = getattr(self.exchange, "close", None)
            if callable(close):
                close()

    def _attempt(self, function, *args: Any, **kwargs: Any):
        with self.lock:
            if self._closed:
                raise ProviderClientClosed(f"{self.identity} client is closed")
            return function(*args, **kwargs)
