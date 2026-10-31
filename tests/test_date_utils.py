# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
from datetime import datetime, timezone
import pytest
import pyarrow as pa

from getfactormodels.utils.date_utils import (
    offset_period_eom,
    parse_quarterly_dates,
    validate_date_range,
    _roll_to_eom,
    _validate_input_date,
)


@pytest.mark.parametrize(
    "freq,input_dates,expected",
    [
        ('m', ["20230115", "20230210"], ["2023-01-31", "2023-02-28"]),
        ('q', ["20230215", "20230510"], ["2023-03-31", "2023-06-30"]),
        ('y', ["20230415"], ["2023-12-31"]),
        ('d', ["2023-01-15"], ["2023-01-15"]),
        ('y', ["2023"], ["2023-12-31"]),
        ('m', ["20230215"], ["2023-02-28"]),
    ]
)
def test_offset_period_eom(freq, input_dates, expected):
    """Test date shifting across frequencies and empty table edge cases."""
    # Test valid conversions
    t = pa.Table.from_pydict({"date": input_dates})
    assert offset_period_eom(t, frequency=freq).column(0).cast(pa.string()).to_pylist() == expected

    # Test empty table edge case (num_columns == 0)
    empty_table = pa.Table.from_arrays([], names=[])
    assert offset_period_eom(empty_table, frequency=freq).num_rows == 0


@pytest.mark.parametrize(
    "input_data,expected",
    [
        (pa.Table.from_pydict({"year": [2022, 2023], "period": [1, 4], "val": [10.5, 20.1]}), ["2022-03-31", "2023-12-31"]),
        (pa.Table.from_pydict({"qtr_code": ["20231", "20232"], "val": [1.0, 2.0]}), ["2023-03-31", "2023-06-30"]),
    ]
)
def test_parse_quarterly_dates(input_data, expected):
    """Test parsing quarterly tables from 1 or 2 columns and empty table checks."""
    res = parse_quarterly_dates(input_data)
    res = offset_period_eom(res, frequency='q')
    assert res.column("date").cast(pa.string()).to_pylist() == expected

    # Test empty table edge case
    empty_table = pa.Table.from_arrays([], names=[])
    assert parse_quarterly_dates(empty_table).num_rows == 0


def test_parse_quarterly_dates_invalid():
    """Test invalid quarterly data parsing triggers ValueError."""
    t = pa.Table.from_pydict({"year": ["invalid"], "period": [5]})
    with pytest.raises(ValueError, match="Failed to parse quarterly dates"):
        parse_quarterly_dates(t)


@pytest.mark.parametrize(
    "raw_input,is_end,expected",
    [
        ("2023", False, "2023-01-01"),
        ("2023", True, "2023-12-31"),
        ("2023-02", True, "2023-02-28"),
        ("2024-02", True, "2024-02-29"),  # Leap year check
        ("2023-04", False, "2023-04-01"),
        ("2023-04", True, "2023-04-30"),
        ("202304", True, "2023-04-30"),
        ("20230415", False, "2023-04-15"),
        ("2023/06/15", False, "2023-06-15"),
        ("2023-07-5", False, "2023-07-05"),
        ("2023-05-20", False, "2023-05-20"),
    ]
)
def test_validate_input_date_formats(raw_input, is_end, expected):
    """Test input date normalization across full, partial, and alternate formats."""
    assert _validate_input_date(raw_input, is_end=is_end) == expected


def test_validate_input_date_edge_cases():
    """Test None handling and invalid date exceptions."""
    assert _validate_input_date(None) is None
    for invalid in ["not-a-date", "2023-13-40"]:
        with pytest.raises(ValueError, match="Invalid date"):
            _validate_input_date(invalid)


def test_validate_date_range(capsys):
    """Test date range validation rules, order checks, and future clamping."""
    today = datetime.now(tz=timezone.utc).date().isoformat()

    # Valid range
    assert validate_date_range("2020-01-01", "2021-01-01") == ("2020-01-01", "2021-01-01")

    # Future end date clamping with warning
    s, e = validate_date_range("2020-01-01", "2099-12-31")
    assert s == "2020-01-01"
    assert e == today
    assert "WARNING: End date is in the future" in capsys.readouterr().err

    # Invalid ranges (start > end or start in future)
    with pytest.raises(ValueError, match="is after end_date"):
        validate_date_range("2022-01-01", "2021-01-01")

    with pytest.raises(ValueError, match="Start date is in the future"):
        validate_date_range("2099-01-01", "2099-12-31")


def test_roll_to_eom():
    """Test rolling datetime forward to the end of the month."""
    assert _roll_to_eom(datetime(2023, 2, 10)) == "2023-02-28"
