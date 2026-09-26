from datetime import date, datetime

import pytest
from fastapi import HTTPException

from src.tools.tq_metadata_conversion import (
    CST,
    EPOCH,
    MetadataContext,
    calendar_range,
    mapping_range,
    parse_holidays,
    parse_mapping,
    trading_candidate,
)
from Test.helpers.tq_metadata_reference import reference

HOLIDAYS = ["2010-01-01", "2024-02-12", "2026-09-25", "2026-10-07"]
EVENTS = {
    "SHFE.rb": [
        ["20260701", "SHFE.rb2609"],
        ["20260924", "SHFE.rb2610"],
        ["20260928", "SHFE.rb2701"],
    ]
}
SYMBOL = "KQ.m@SHFE.rb"


def ns(value: str) -> int:
    parsed = datetime.fromisoformat(value).replace(tzinfo=CST)
    delta = parsed - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000


def context():
    return MetadataContext(
        ns("2026-09-26T13:00:00") // 1_000_000,
        parse_holidays(HOLIDAYS),
        parse_mapping(EVENTS),
        ns("2026-09-24T14:55:00"),
    )


def test_unsorted_holiday_max_is_actual_date_not_december_31():
    source = parse_holidays([*reversed(HOLIDAYS), HOLIDAYS[0]])
    assert source.last == date(2026, 10, 7)
    assert source.start == date(2010, 1, 1) and source.end == date(2026, 12, 31)
    result = calendar_range(date(2024, 2, 28), date(2024, 3, 2), source, 1)
    assert [row.date.day for row in result.records] == [28, 29, 1, 2]
    assert result.records[-1].trading is False
    with pytest.raises(HTTPException, match="TQ_CALENDAR_RANGE_UNAVAILABLE"):
        calendar_range(date(2027, 1, 1), date(2027, 1, 2), source, 1)


def test_weekend_preannouncement_does_not_change_current_verification():
    ctx = context()
    result = mapping_range(SYMBOL, date(2026, 9, 23), date(2026, 9, 24), ctx)
    assert result.facts.verified_date == date(2026, 9, 24)
    assert [row.underlying_symbol for row in result.records] == [
        "SHFE.rb2609",
        "SHFE.rb2610",
    ]
    assert [node.date for node in result.nodes] == [date(2026, 7, 1), date(2026, 9, 24)]
    assert result.nodes[-1].old_symbol == "SHFE.rb2609"
    with pytest.raises(HTTPException, match="TQ_MAPPING_RANGE_UNAVAILABLE"):
        mapping_range(SYMBOL, date(2026, 9, 23), date(2026, 9, 28), ctx)
    assert (
        mapping_range(SYMBOL, date(2026, 9, 25), date(2026, 9, 26), ctx).records == []
    )


def test_actual_night_session_can_verify_next_trading_day():
    ctx = context()
    ctx.server_time = ns("2026-09-28T21:03:00") // 1_000_000
    ctx.reference_time = ns("2026-09-28T21:00:00")
    result = mapping_range(SYMBOL, date(2026, 9, 29), date(2026, 9, 29), ctx)
    assert result.facts.verified_date == date(2026, 9, 29)
    assert result.nodes[0].date == date(2026, 9, 28)
    assert trading_candidate(ns("2026-09-18T21:00:00")) == date(2026, 9, 21)


def test_bar_created_after_clock_request_keeps_same_valid_business_day():
    ctx = context()
    ctx.server_time = ns("2026-09-24T14:54:59") // 1_000_000
    result = mapping_range(SYMBOL, date(2026, 9, 24), date(2026, 9, 24), ctx)
    assert result.facts.verified_date == date(2026, 9, 24)
    ctx.reference_time = ns("2026-09-29T14:55:00")
    with pytest.raises(HTTPException, match="TQ_MAPPING_REFERENCE_UNAVAILABLE"):
        mapping_range(SYMBOL, date(2026, 9, 24), date(2026, 9, 24), ctx)


def test_conversion_matches_official_algorithm_in_effective_range():
    events = {
        "SHFE.rb": [
            ["20240901", ""],
            ["20240907", "SHFE.rb2501"],
            ["20240915", "SHFE.rb2505"],
        ]
    }
    start, end = date(2024, 9, 1), date(2024, 9, 20)
    ctx = MetadataContext(
        ns("2024-09-20T17:00:00") // 1_000_000,
        parse_holidays(HOLIDAYS),
        parse_mapping(events),
        ns("2024-09-20T14:55:00"),
    )
    expected_calendar, expected_mapping = reference(
        HOLIDAYS, events, start, end, SYMBOL
    )
    assert ctx.calendar is not None
    actual_calendar = calendar_range(start, end, ctx.calendar, ctx.server_time)
    actual = mapping_range(SYMBOL, start, end, ctx)
    assert [
        (row.date, row.trading) for row in actual_calendar.records
    ] == expected_calendar
    assert [(row.date, row.underlying_symbol) for row in actual.records] == [
        (d, u) for d, u in expected_mapping if u
    ]
    assert actual.nodes[0].date == date(2024, 9, 9)
    assert actual.nodes[0].old_symbol is None
    assert actual.nodes[1].old_symbol == "SHFE.rb2501"


def test_nontrading_events_never_create_untraded_old_contract():
    ctx = context()
    ctx.mapping = parse_mapping(
        {
            "SHFE.rb": [
                ["20260701", "SHFE.rb2608"],
                ["20260919", "SHFE.rb2609"],
                ["20260920", "SHFE.rb2610"],
            ]
        }
    )
    result = mapping_range(SYMBOL, date(2026, 9, 23), date(2026, 9, 24), ctx)
    assert result.nodes[0].date == date(2026, 9, 21)
    assert result.nodes[0].old_symbol == "SHFE.rb2608"


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"SHFE.rb": [["20260101", "x"]]},
        {"SHFE.rb": [["20260101", "SHFE.rb2605"], ["20260101", "SHFE.rb2610"]]},
        {"SHFE.rb": [["20260101", "SHFE.rb2605"], ["20260102", ""]]},
    ],
)
def test_invalid_sources_fail_without_filtering_internal_errors(raw):
    with pytest.raises(HTTPException, match="TQ_METADATA_INVALID_SOURCE"):
        parse_mapping(raw)


def test_unknown_left_node_and_invalid_reference_are_explicit():
    ctx = context()
    ctx.mapping = parse_mapping({"SHFE.rb": [["20090101", "SHFE.rb2610"]]})
    with pytest.raises(HTTPException, match="TQ_MAPPING_CONTEXT_UNAVAILABLE"):
        mapping_range(SYMBOL, date(2026, 9, 23), date(2026, 9, 24), ctx)
    ctx.reference_time = None
    with pytest.raises(HTTPException, match="TQ_MAPPING_REFERENCE_UNAVAILABLE"):
        mapping_range(SYMBOL, date(2026, 9, 23), date(2026, 9, 24), ctx)
