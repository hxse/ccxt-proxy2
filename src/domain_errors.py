class DomainError(Exception):
    status_code = 500
    code = "DOMAIN_ERROR"

    def __init__(self, message: str | None = None):
        self.message = message
        detail: dict[str, str] = {"code": self.code}
        if message:
            detail["message"] = message
        self.detail = detail
        super().__init__(message or self.code)


class CapabilityNotSupported(DomainError):
    status_code = 422
    code = "NOT_SUPPORTED"


class ResponseRowLimitExceeded(DomainError):
    status_code = 422
    code = "RESPONSE_ROW_LIMIT_EXCEEDED"


class NetworkIncomplete(DomainError):
    status_code = 502
    code = "NETWORK_INCOMPLETE"


class InvalidProviderData(DomainError):
    status_code = 502
    code = "INVALID_PROVIDER_DATA"


class CacheCapacityExceeded(DomainError):
    status_code = 507
    code = "CACHE_CAPACITY_EXCEEDED"
