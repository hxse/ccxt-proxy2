from pathlib import Path

import pytest

from src.tools.tq_errors import TqLegacyMetadataCallForbidden
from src.tools.tq_metadata_boundary import (
    ADAPTER,
    REFERENCE,
    repository_violations,
    violations,
)
from src.tools.tq_trading_status import TradingStatusTqApi


@pytest.mark.parametrize(
    "source",
    [
        "api.get_trading_calendar(a,b)",
        "f=api.query_his_cont_quotes; f(s)",
        "getattr(api, 'get_trading_calendar')(a,b)",
        "super().query_his_cont_quotes(s)",
        "TqApi.get_trading_calendar(api,a,b)",
        "from tqsdk import TqApi as Raw\na=Raw()",
        "import tqsdk as q\na=q.TqApi()",
        "from tqsdk.calendar import TqContCalendar as Cal\nCal(a,b)",
    ],
)
def test_representative_legacy_bypasses_are_rejected(source):
    assert violations(source, "src/tools/bypass.py")
    assert violations(source, "Test/helpers/not_the_reference.py")


def test_adapter_definitions_are_allowed_but_calls_to_base_are_not():
    assert (
        violations("def get_trading_calendar(self): raise RuntimeError()", ADAPTER)
        == []
    )
    assert violations(
        "def get_trading_calendar(self): return super().get_trading_calendar()", ADAPTER
    )


def test_production_repository_has_no_legacy_call_path():
    assert repository_violations(Path(__file__).resolve().parents[1]) == []


@pytest.mark.parametrize(
    "method", ["get_trading_calendar", "query_his_cont_quotes", "query_symbol_info"]
)
def test_runtime_legacy_methods_fail_before_sdk_or_network(method):
    api = object.__new__(TradingStatusTqApi)
    with pytest.raises(TqLegacyMetadataCallForbidden, match=method):
        getattr(api, method)()


@pytest.mark.parametrize(
    "source",
    [
        "api.query_symbol_info(s)",
        "f=api.query_symbol_info; f(s)",
        "getattr(api, 'query_symbol_info')(s)",
        "super().query_symbol_info(s)",
        "api.get_quote(s).underlying_symbol",
        "api.get_quote_list([s])[0].underlying_symbol",
        "getattr(api, 'get_quote')(s)",
        "api.query_graphql(q, v)",
        "client.resolve_underlying(s)",
    ],
)
def test_current_mapping_bypasses_are_forbidden_even_in_reference(source):
    assert violations(source, "src/tools/bypass.py")
    assert violations(source, REFERENCE)


def test_reference_only_allows_offline_calendar_conversion():
    assert not violations(
        "from tqsdk import calendar\ncalendar.TqContCalendar(a,b)", REFERENCE
    )
    assert violations("from tqsdk import TqApi as Raw\na=Raw()", REFERENCE)
