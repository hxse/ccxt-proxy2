from fastapi import HTTPException
from loguru import logger


INTERNAL_SERVER_ERROR_DETAIL = "internal server error"


def request_logger(route: str, params: object | None = None):
    context = {"route": route}
    if params is not None:
        for field in (
            "exchange_name",
            "market",
            "mode",
            "symbol",
            "id",
            "timeframe",
            "side",
        ):
            value = getattr(params, field, None)
            if value is not None:
                context[field] = value

        symbols = getattr(params, "symbols", None)
        if symbols:
            context["symbols"] = symbols

    return logger.bind(**context)


def internal_server_error(route: str, params: object | None = None) -> HTTPException:
    request_logger(route, params).exception(f"{route} failed")
    return HTTPException(status_code=500, detail=INTERNAL_SERVER_ERROR_DETAIL)
