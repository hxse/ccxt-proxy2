import math

import pytest

from src.cache_tool.models import OhlcvResult, canonical_row, canonical_rows


def _row(timestamp: int, close: float = 1.5):
    return (timestamp, 1.0, 2.0, 0.0, close, 5.0)


@pytest.mark.parametrize("completion", [True, False])
def test_non_empty_result_requires_boolean_completion(completion):
    result = OhlcvResult([_row(10)], completion)

    assert result.last_bar_completion_confirmed is completion


def test_result_rejects_metadata_that_disagrees_with_empty_state():
    with pytest.raises(ValueError, match="empty OHLCV result must use null"):
        OhlcvResult([], False)
    with pytest.raises(ValueError, match="non-empty OHLCV result requires"):
        OhlcvResult([_row(10)], None)


def test_result_has_no_response_tail_transform():
    assert not hasattr(OhlcvResult, "for_response")


def test_canonical_rows_sort_deduplicate_and_keep_latest_revision():
    revised = _row(20, close=1.75)

    rows = canonical_rows([_row(20), _row(10), revised])

    assert [row[0] for row in rows] == [10, 20]
    assert rows[-1][4] == 1.75


@pytest.mark.parametrize(
    "row",
    [
        (True, 1.0, 2.0, 0.0, 1.5, 5.0),
        (-1, 1.0, 2.0, 0.0, 1.5, 5.0),
        (10, 1.0, 0.0, 2.0, 1.5, 5.0),
        (10, 3.0, 2.0, 0.0, 1.5, 5.0),
        (10, 1.0, 2.0, 0.0, 3.0, 5.0),
        (10, 1.0, 2.0, 0.0, 1.5, -1.0),
        (10, 1.0, math.inf, 0.0, 1.5, 5.0),
    ],
)
def test_canonical_row_rejects_unsafe_financial_values(row):
    with pytest.raises(ValueError):
        canonical_row(row)
