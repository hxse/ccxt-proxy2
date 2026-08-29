import math

import pandas as pd
import pytest
from fastapi import Depends, FastAPI, HTTPException
from pydantic import ValidationError

from src.tools.config_types import TqConfig
from src.tools.tq_data_source import (
    TqDataFrameError,
    clean_tq_serial_records,
    history_wide_frame_to_records,
    normalize_tq_serial_frame,
)
from src.tools.tq_manager import TqManager
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


class FakeTqApi:
    def __init__(self):
        self.calls: list[str] = []

    def get_kline_serial(self, symbol, duration_seconds, data_length, adj_type=None):
        self.calls.append("get_kline_serial")
        return pd.DataFrame(
            {
                "datetime": [1718000000000000000],
                "open": [3600.0],
                "high": [3605.0],
                "low": [3599.0],
                "close": [3602.0],
            }
        )

    def get_tick_serial(self, symbol, data_length, adj_type=None):
        self.calls.append("get_tick_serial")
        return pd.DataFrame(
            {
                "datetime": [1718000000000000000],
                "last_price": [3605.0],
                "average": [3600.0],
                "highest": [3610.0],
                "lowest": [3590.0],
                "bid_price1": [3604.0],
                "ask_price1": [3605.0],
            }
        )

    def wait_update(self, deadline=None):
        self.calls.append("wait_update")
        assert deadline is not None
        return False


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


def test_kline_trims_leading_placeholders_and_allows_short_response():
    frame = pd.DataFrame(
        {
            "id": [-1, 10, 11],
            "datetime": [0, 1718000000000000000, 1718000060000000000],
            "open": [math.nan, 3600.0, 3601.0],
            "high": [math.nan, 3605.0, 3606.0],
            "low": [math.nan, 3599.0, 3600.0],
            "close": [math.nan, 3602.0, 3603.0],
            "volume": [0, 100, 110],
            "open_oi": [0, 1000, 1002],
            "close_oi": [0, 1001, 1003],
        }
    )

    result = clean_tq_serial_records(frame, "kline")

    assert len(result) == 2
    assert result[0]["datetime"] == 1718000000000000000
    assert result[0]["open"] == 3600.0


def test_kline_middle_placeholder_is_not_silently_removed():
    frame = pd.DataFrame(
        {
            "datetime": [1718000000000000000, 0, 1718000060000000000],
            "open": [3600.0, math.nan, 3601.0],
            "high": [3605.0, math.nan, 3606.0],
            "low": [3599.0, math.nan, 3600.0],
            "close": [3602.0, math.nan, 3603.0],
        }
    )

    with pytest.raises(TqDataFrameError):
        normalize_tq_serial_frame(frame, "kline")


def test_kline_rejects_non_increasing_datetime():
    frame = pd.DataFrame(
        {
            "datetime": [1718000000000000000, 1718000000000000000],
            "open": [3600.0, 3601.0],
            "high": [3605.0, 3606.0],
            "low": [3599.0, 3600.0],
            "close": [3602.0, 3603.0],
        }
    )

    with pytest.raises(TqDataFrameError):
        normalize_tq_serial_frame(frame, "kline")


def test_tick_trims_leading_placeholder_using_price_fields_not_volume():
    frame = pd.DataFrame(
        {
            "datetime": [0, 1718000000000000000],
            "last_price": [math.nan, 3605.0],
            "average": [math.nan, 3600.0],
            "highest": [math.nan, 3610.0],
            "lowest": [math.nan, 3590.0],
            "bid_price1": [math.nan, 3604.0],
            "ask_price1": [math.nan, 3605.0],
            "volume": [0, 12345],
            "amount": [math.nan, 4567890.0],
            "open_interest": [0, 100000],
        }
    )

    result = clean_tq_serial_records(frame, "tick")

    assert len(result) == 1
    assert result[0]["datetime"] == 1718000000000000000
    assert result[0]["last_price"] == 3605.0


def test_serialization_converts_nan_and_inf_to_none():
    frame = pd.DataFrame(
        {
            "datetime": [1718000000000000000],
            "open": [3600.0],
            "high": [float("inf")],
            "low": [3599.0],
            "close": [3602.0],
            "volume": [math.nan],
        }
    )

    result = clean_tq_serial_records(frame, "kline")

    assert result[0]["high"] is None
    assert result[0]["volume"] is None


def test_serial_rejects_row_when_every_price_is_non_finite():
    frame = pd.DataFrame(
        {
            "datetime": [1718000000000000000],
            "open": [float("inf")],
            "high": [float("inf")],
            "low": [float("-inf")],
            "close": [float("nan")],
        }
    )

    with pytest.raises(TqDataFrameError):
        normalize_tq_serial_frame(frame, "kline")


def test_history_wide_frame_converts_to_long_records():
    frame = pd.DataFrame(
        {
            "date": ["2026-06-10", "2026-06-11"],
            "KQ.m@DCE.i": ["DCE.i2509", "DCE.i2510"],
            "KQ.m@DCE.a": ["DCE.a2509", ""],
            "KQ.m@DCE.b": [math.nan, math.nan],
            "KQ.m@DCE.c": ["NaN", None],
        }
    )

    result = history_wide_frame_to_records(frame)

    assert result == [
        {
            "date": "2026-06-10",
            "symbol": "KQ.m@DCE.i",
            "underlying_symbol": "DCE.i2509",
        },
        {
            "date": "2026-06-11",
            "symbol": "KQ.m@DCE.i",
            "underlying_symbol": "DCE.i2510",
        },
        {
            "date": "2026-06-10",
            "symbol": "KQ.m@DCE.a",
            "underlying_symbol": "DCE.a2509",
        },
    ]


def test_fetch_ohlcv_and_tick_advance_tq_message_loop(temp_dir):
    fake_api = FakeTqApi()
    manager = TqManager(
        TqConfig(),
        lock_path=temp_dir / "tq.lock",
        update_timeout_seconds=0.0,
    )
    manager._api = fake_api

    manager.fetch_ohlcv(
        TqOhlcvRequest(symbol="SHFE.rb2505", duration_seconds=60, data_length=10)
    )
    manager.fetch_tick(TqTickRequest(symbol="SHFE.rb2505", data_length=10))

    assert fake_api.calls == [
        "get_kline_serial",
        "wait_update",
        "get_tick_serial",
        "wait_update",
    ]


def test_tq_ohlcv_request_preserves_repeated_symbol_params():
    request = tq_ohlcv_request(
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
        tq_tick_request(symbol="SHFE.rb2505", data_length=10001)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_DATA_LENGTH"

    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(symbol=[""], duration_seconds=60)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_SYMBOL"

    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(symbol=["SHFE.rb2505"], duration_seconds=0)
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_DURATION_SECONDS"


def test_tq_http_invalid_adj_type_uses_documented_error():
    with pytest.raises(HTTPException) as exc_info:
        tq_ohlcv_request(symbol=["SHFE.rb2505"], duration_seconds=60, adj_type="X")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_ADJ_TYPE"

    with pytest.raises(HTTPException) as exc_info:
        tq_tick_request(symbol="SHFE.rb2505", adj_type="X")
    assert exc_info.value.status_code == 400
    assert exc_info.value.detail == "TQ_INVALID_ADJ_TYPE"


def test_tq_openapi_query_schema_documents_bounds_and_adj_type_enum():
    schema = _build_tq_dependency_app().openapi()

    ohlcv_params = {
        parameter["name"]: parameter["schema"]
        for parameter in schema["paths"]["/ohlcv"]["get"]["parameters"]
    }
    tick_params = {
        parameter["name"]: parameter["schema"]
        for parameter in schema["paths"]["/tick"]["get"]["parameters"]
    }
    underlying_params = {
        parameter["name"]: parameter["schema"]
        for parameter in schema["paths"]["/underlying"]["get"]["parameters"]
    }

    assert ohlcv_params["duration_seconds"]["exclusiveMinimum"] == 0
    assert ohlcv_params["data_length"]["minimum"] == 1
    assert ohlcv_params["data_length"]["maximum"] == MAX_TQ_DATA_LENGTH
    assert ohlcv_params["adj_type"]["enum"] == TQ_ADJ_TYPE_QUERY_ENUM
    assert tick_params["data_length"]["minimum"] == 1
    assert tick_params["data_length"]["maximum"] == MAX_TQ_DATA_LENGTH
    assert tick_params["adj_type"]["enum"] == TQ_ADJ_TYPE_QUERY_ENUM
    assert underlying_params["n"]["exclusiveMinimum"] == 0


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
        symbol=["KQ.m@DCE.i", "KQ.m@DCE.a"],
        n=20,
    )

    assert request.symbol == ["KQ.m@DCE.i", "KQ.m@DCE.a"]
    assert request.n == 20


def test_underlying_items_require_cont_symbol(temp_dir):
    manager = TqManager(None, lock_path=temp_dir / "tq.lock")
    item = manager._underlying_item_from_row(
        "DCE.i2509",
        {
            "underlying_symbol": "",
            "ins_class": "FUTURE",
            "exchange_id": "DCE",
            "product_id": "i",
        },
    )

    with pytest.raises(HTTPException) as exc_info:
        manager._validate_underlying_item(item)

    assert exc_info.value.status_code == 422
    assert exc_info.value.detail == "TQ_NOT_CONT_SYMBOL"


def test_tq_exception_mapping_prefers_adj_type_over_generic_param_error(temp_dir):
    manager = TqManager(None, lock_path=temp_dir / "tq.lock")

    exc = manager._map_tq_exception(Exception("参数错误，多合约 K 线序列不支持复权。"))

    assert exc.status_code == 400
    assert exc.detail == "TQ_INVALID_ADJ_TYPE"


def test_tq_request_models_keep_thin_forward_fields_only():
    fields = set(TqOhlcvRequest.model_fields)

    assert fields == {"symbol", "duration_seconds", "data_length", "adj_type"}
    assert "since" not in fields
    assert "limit" not in fields
    assert "enable_cache" not in fields
