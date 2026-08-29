from typing import Any, cast

import pytest
from fastapi import Depends, FastAPI, HTTPException
from pydantic import ValidationError
from starlette.requests import Request

from src.types_tq import (
    MAX_TQ_DATA_LENGTH,
    TQ_ADJ_TYPE_QUERY_ENUM,
    TqOhlcvRequest,
    TqTickRequest,
    TqUnderlyingSymbolRequest,
    tq_ohlcv_request,
    tq_tick_request,
    tq_underlying_symbol_request,
)


def _query_request(query_string: bytes = b"") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/tq",
            "headers": [],
            "query_string": query_string,
        }
    )


def _build_tq_dependency_app() -> FastAPI:
    app = FastAPI()

    @app.get("/ohlcv")
    def ohlcv(params: TqOhlcvRequest = Depends(tq_ohlcv_request)):
        return params.model_dump()

    @app.get("/tick")
    def tick(params: TqTickRequest = Depends(tq_tick_request)):
        return params.model_dump()

    @app.get("/underlying")
    def underlying(
        params: TqUnderlyingSymbolRequest = Depends(tq_underlying_symbol_request),
    ):
        return params.model_dump()

    return app


def test_tq_ohlcv_request_preserves_repeated_symbol_params():
    request = tq_ohlcv_request(
        _query_request(),
        symbol=["SHFE.au2508", "CFFEX.IF2506"],
        duration_seconds=60,
        data_length=3000,
        adj_type="FORWARD",
    )

    assert request.symbol == ["SHFE.au2508", "CFFEX.IF2506"]
    assert request.duration_seconds == 60
    assert request.data_length == 3000
    assert request.adj_type == "FORWARD"


def test_tq_request_validation_uses_documented_http_errors():
    with pytest.raises(HTTPException) as exc_info:
        tq_tick_request(_query_request(), symbol="SHFE.rb2505", data_length=10001)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_DATA_LENGTH"

    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(_query_request(), symbol=[""], duration_seconds=60)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_SYMBOL"

    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(_query_request(), symbol=["SHFE.rb2505"], duration_seconds=0)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_DURATION_SECONDS"


def test_tq_http_invalid_adj_type_uses_documented_error():
    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(
            _query_request(),
            symbol=["SHFE.rb2505"],
            duration_seconds=60,
            adj_type="X",
        )
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_ADJ_TYPE"

    with pytest.raises(HTTPException) as exc_info:
        tq_tick_request(_query_request(), symbol="SHFE.rb2505", adj_type="X")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_ADJ_TYPE"


def test_tq_openapi_query_schema_documents_bounds_and_adj_type_enum():
    schema = _build_tq_dependency_app().openapi()
    paths = schema["paths"]
    ohlcv = {
        item["name"]: item["schema"] for item in paths["/ohlcv"]["get"]["parameters"]
    }
    tick = {
        item["name"]: item["schema"] for item in paths["/tick"]["get"]["parameters"]
    }
    underlying = {
        item["name"]: item["schema"]
        for item in paths["/underlying"]["get"]["parameters"]
    }

    assert ohlcv["duration_seconds"]["exclusiveMinimum"] == 0
    assert ohlcv["data_length"]["minimum"] == 1
    assert ohlcv["data_length"]["maximum"] == MAX_TQ_DATA_LENGTH
    assert ohlcv["adj_type"]["enum"] == TQ_ADJ_TYPE_QUERY_ENUM
    assert tick["data_length"]["minimum"] == 1
    assert tick["data_length"]["maximum"] == MAX_TQ_DATA_LENGTH
    assert tick["adj_type"]["enum"] == TQ_ADJ_TYPE_QUERY_ENUM
    assert underlying["n"]["exclusiveMinimum"] == 0


def test_tq_request_models_enforce_internal_contract():
    with pytest.raises(ValidationError):
        TqOhlcvRequest(symbol="", duration_seconds=60)
    with pytest.raises(ValidationError):
        TqOhlcvRequest(symbol="SHFE.rb2505", duration_seconds=0)
    with pytest.raises(ValidationError):
        TqTickRequest(symbol="SHFE.rb2505", data_length=10001)
    with pytest.raises(ValidationError):
        TqTickRequest(symbol=" ", data_length=10)
    with pytest.raises(ValidationError):
        TqUnderlyingSymbolRequest(symbol="KQ.m@DCE.i", n=0)


def test_underlying_request_preserves_repeated_symbol_params():
    request = tq_underlying_symbol_request(
        _query_request(),
        symbol=["KQ.m@DCE.i", "KQ.m@DCE.a"],
        n=20,
    )

    assert request.symbol == ["KQ.m@DCE.i", "KQ.m@DCE.a"]
    assert request.n == 20


def test_tq_dependency_rejects_unknown_query_parameters():
    with pytest.raises(HTTPException) as exc_info:
        tq_tick_request(
            _query_request(b"symbol=SHFE.rb2505&typo=1"),
            symbol="SHFE.rb2505",
        )

    detail = cast(list[dict[str, Any]], exc_info.value.detail)
    assert exc_info.value.status_code == 422
    assert detail[0]["loc"] == ["query", "typo"]
