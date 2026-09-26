import threading
import time

import pytest
from fastapi import HTTPException

from src.tools.tq_client import TqClient
from src.tools.tq_metadata_sdk import reference_time
from Test.test_tq_metadata_conversion import SYMBOL, ns
from Test.test_tq_ohlcv_cache import SerialApi


def test_reference_uses_one_uncached_main_bar_without_current_mapping():
    api = SerialApi()
    api.rows = [dict(api.rows[0], datetime=ns("2026-09-24T14:55:00"))]
    try:
        timestamp = reference_time(api, SYMBOL, time.monotonic() + 1, threading.Event())
        assert timestamp == ns("2026-09-24T14:55:00")
        assert api.calls == [(SYMBOL, 300, 1, None)]
    finally:
        api.close()


@pytest.mark.parametrize("case", ["empty", "extra", "invalid_time", "invalid_prices"])
def test_invalid_reference_is_explicit(case):
    api = SerialApi()
    api.rows = [api.rows[0]]
    if case == "empty":
        api.rows = []
    elif case == "extra":
        api.rows *= 2
    elif case == "invalid_time":
        api.rows[0]["datetime"] = -1
    else:
        api.rows[0]["high"] = 0
    try:
        with pytest.raises(HTTPException) as captured:
            reference_time(api, SYMBOL, time.monotonic() + 1, threading.Event())
        assert (captured.value.status_code, captured.value.detail) == (
            502,
            "TQ_MAPPING_REFERENCE_UNAVAILABLE",
        )
    finally:
        api.close()


def test_forbidden_sdk_error_is_internal_misuse_not_network(tmp_path):
    from src.tools.tq_errors import TqLegacyMetadataCallForbidden

    client = TqClient(None, lock_path=tmp_path / "unused.lock")
    error = client._map_tq_exception(TqLegacyMetadataCallForbidden("query_symbol_info"))
    assert (error.status_code, error.detail) == (
        500,
        "TQ_LEGACY_METADATA_CALL_FORBIDDEN",
    )
