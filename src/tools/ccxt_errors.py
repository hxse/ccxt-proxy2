import ccxt

from src.domain_errors import (
    CapabilityNotSupported,
    DomainError,
    InvalidProviderRequest,
    NetworkIncomplete,
    OperationStatusUnknown,
    ProviderAuthenticationFailed,
    ProviderFailure,
    ProviderInsufficientFunds,
    ProviderOperationRejected,
    ProviderOrderNotFound,
    ProviderOrderRejected,
)


def map_ccxt_exception(exc: ccxt.BaseError) -> DomainError:
    """Map CCXT's exception hierarchy to the stable HTTP/domain contract."""
    if isinstance(exc, ccxt.CancelPending):
        return OperationStatusUnknown(
            "provider reports that operation status is unknown"
        )
    if isinstance(exc, ccxt.OrderNotFound):
        return ProviderOrderNotFound("provider order was not found")
    if isinstance(exc, ccxt.InvalidOrder):
        return ProviderOrderRejected("provider rejected the order")
    if isinstance(exc, ccxt.InsufficientFunds):
        return ProviderInsufficientFunds("provider reported insufficient funds")
    if isinstance(exc, ccxt.AuthenticationError):
        return ProviderAuthenticationFailed(
            "provider authentication or permission check failed"
        )
    if isinstance(exc, ccxt.NotSupported):
        return CapabilityNotSupported("provider does not support this operation")
    if isinstance(exc, (ccxt.ArgumentsRequired, ccxt.BadRequest)):
        return InvalidProviderRequest("provider rejected the request parameters")
    if isinstance(exc, ccxt.OperationRejected):
        return ProviderOperationRejected("provider rejected the operation")
    if isinstance(exc, ccxt.NetworkError):
        return NetworkIncomplete("provider network operation failed")
    return ProviderFailure("provider operation failed")
