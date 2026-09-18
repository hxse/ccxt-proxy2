import asyncio
from datetime import date
from typing import Any, cast

import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException
from pydantic import ValidationError
from starlette.requests import Request

from src.router.auth_handler import manager as auth_manager
from src.router.tq_router import tq_router
from src.tools.tq_client import TqClient
from src.tools.tq_manager import tq_manager
from src.types_tq import (
    MAX_TQ_DATA_LENGTH,
    TQ_ADJ_TYPE_QUERY_ENUM,
    TqOhlcvRequest,
    TqTickRequest,
    TqTradingCalendarRequest,
    TqUnderlyingSymbolRequest,
    tq_ohlcv_request,
    tq_tick_request,
    tq_trading_calendar_request,
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

    @app.get("/calendar")
    def calendar(
        params: TqTradingCalendarRequest = Depends(tq_trading_calendar_request),
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

    calendar = {item["name"]: item for item in paths["/calendar"]["get"]["parameters"]}
    assert calendar["start_date"]["schema"]["format"] == "date"
    assert calendar["end_date"]["schema"]["format"] == "date"
    for name in ("start_date", "end_date"):
        description = calendar[name]["description"]
        assert "Asia/Shanghai" in description
        assert "YYYY-MM-DD" in description
        assert "UTC" in description
        assert "不做时区换算" in description


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
    with pytest.raises(ValidationError, match="TQ_INVALID_DATE_RANGE"):
        TqTradingCalendarRequest(
            start_date=date(2026, 9, 2),
            end_date=date(2026, 9, 1),
        )


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


def test_trading_calendar_request_validates_range_and_unknown_query():
    request = tq_trading_calendar_request(
        _query_request(),
        start_date=date(2026, 9, 1),
        end_date=date(2026, 9, 3),
    )
    assert request.start_date == date(2026, 9, 1)
    assert request.end_date == date(2026, 9, 3)

    with pytest.raises(HTTPException) as reversed_range:
        tq_trading_calendar_request(
            _query_request(),
            start_date=date(2026, 9, 3),
            end_date=date(2026, 9, 1),
        )
    assert reversed_range.value.status_code == 400
    assert reversed_range.value.detail == "TQ_INVALID_DATE_RANGE"

    with pytest.raises(HTTPException) as unknown:
        tq_trading_calendar_request(
            _query_request(b"start_date=2026-09-01&end_date=2026-09-03&typo=1"),
            start_date=date(2026, 9, 1),
            end_date=date(2026, 9, 3),
        )
    assert unknown.value.status_code == 422


def test_calendar_coverage_error_maps_to_stable_code(temp_dir):
    manager = TqClient(None, lock_path=temp_dir / "tq.lock")
    error = manager._map_tq_exception(
        Exception("交易日历可以处理的范围为 2010-01-01 ～ 2026-12-31，请修改参数")
    )

    assert error.status_code == 422
    assert error.detail == "TQ_CALENDAR_RANGE_UNAVAILABLE"


def test_trading_calendar_http_route_parses_dates_and_serializes_response(
    monkeypatch,
):
    captured: list[TqTradingCalendarRequest] = []

    def fetch(request: TqTradingCalendarRequest):
        captured.append(request)
        return [
            {"date": "2026-09-11", "trading": True},
            {"date": "2026-09-12", "trading": False},
        ]

    monkeypatch.setattr(tq_manager, "fetch_trading_calendar", fetch)
    app = FastAPI()
    app.include_router(tq_router)
    app.dependency_overrides[auth_manager] = lambda: {"sub": "test"}

    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as client:
            return await client.get(
                "/tq/fetch_trading_calendar",
                params={"start_date": "2026-09-11", "end_date": "2026-09-12"},
            )

    response = asyncio.run(request())

    assert response.status_code == 200
    assert response.json() == [
        {"date": "2026-09-11", "trading": True},
        {"date": "2026-09-12", "trading": False},
    ]
    assert captured[0].start_date == date(2026, 9, 11)
    assert captured[0].end_date == date(2026, 9, 12)
