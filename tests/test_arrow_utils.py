#!/usr/bin/env python3
# getfactormodels: A Python package to retrieve financial factor model data.
# Copyright (C) 2025 S. Martin <x512@pm.me>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import logging
from datetime import date
import pyarrow as pa
import pytest
from getfactormodels.utils.arrow_utils import (
    _format_for_preview,
    _validate_columns,
    filter_table_by_date,
    print_table_preview,
    rearrange_columns,
    round_to_precision,
    scale_to_decimal,
    select_table_columns,
)

"""Tests for methods in getfactormodels/utils/arrow_utils.py"""

@pytest.fixture
def sample_table():
    """Creates a basic table with dates, factors, and RF."""
    data = {
        "date": [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)],
        "Mkt-RF": [1.1, 2.2, 3.3],
        "SMB": [0.1, 0.2, 0.3],
        "RF": [0.01, 0.02, 0.03],
    }
    return pa.table(data)

@pytest.mark.parametrize("cols, expected", [
    (["A", "B", "Mkt-RF", "RF", "date"], ["date", "Mkt-RF", "A", "B", "RF"]),
    (["RF", "B", "date"], ["date", "B", "RF"]),
    (["date", "Mkt-RF", "B"], ["date", "Mkt-RF", "B"]),
])


def test_rearrange_columns(cols, expected):
    table = pa.table({c: [1.0] for c in cols})
    result = rearrange_columns(table)
    assert result.column_names == expected


def test_validate_columns_errors():
    empty_table = pa.table({"a": []})
    with pytest.raises(RuntimeError, match="Table is empty."):
        _validate_columns(empty_table, ["a"])

    # if not names return []
    assert _validate_columns(pa.table({"a": [1]}), None) == []
    assert _validate_columns(pa.table({"a": [1]}), []) == []


def test_select_table_columns(sample_table):
    selected = select_table_columns(sample_table, ["sMb", "DATE"])
    assert selected.column_names == ["date", "SMB"]

    # Test extract must include at least one factor. 'date' is passed.
    with pytest.raises(ValueError, match="at least one factor."):
        select_table_columns(sample_table, ["date"])

    with pytest.raises(ValueError, match="Columns not found"):
        select_table_columns(sample_table, ["non_existent"])


def test_filter_table_by_date(sample_table):
    # start_date only
    filtered = filter_table_by_date(sample_table, start="2023-01-02", end=None)
    assert filtered.num_rows == 2
    # range
    filtered = filter_table_by_date(sample_table, start="2023-01-02", end="2023-01-02")
    assert filtered.num_rows == 1
    assert filtered.column("date")[0].as_py() == date(2023, 1, 2)
    # No filter
    assert filter_table_by_date(sample_table, None, None).num_rows == 3


def test_scale_to_decimal(sample_table):
    scaled = scale_to_decimal(sample_table)
    # 1.1 to 0.011
    assert scaled.column("Mkt-RF")[0].as_py() == pytest.approx(0.011)


def test_round_to_precision(sample_table):
    data = {"Mkt-RF": [1.1234567], "RF": [0.0123456]}
    table = pa.table(data)

    rounded = round_to_precision(table, precision=6)

    assert len(str(rounded.column("RF")[0].as_py())) <= 6 # 0.0123
    assert rounded.column("Mkt-RF")[0].as_py() == 1.123457


def test_format_for_preview_nans():
    # Test NaNs in preview table
    assert _format_for_preview(None, "Mkt-RF") == "NaN"
    assert _format_for_preview(float('nan'), "SMB") == "NaN"
    assert _format_for_preview(1.23, "date") == "1.23"


def test_print_table_preview_empty(caplog):
    empty_table = pa.table({"date": [], "Mkt-RF": []})

    with caplog.at_level(logging.WARNING):
        result = print_table_preview(empty_table)
    assert result is None
    assert "Empty Table" in caplog.text


def test_print_table_preview_gap():
    # Tests printing the gap "[...]" 
    # Create 20 rows, so gap triggers
    data = {"date": [date(2023, 1, i+1) for i in range(20)], "x": [1.0]*20}
    table = pa.table(data)

    class MockTable:
        def __init__(self, table, prec):
            self.data = table
            self._precision = prec
        def __getattr__(self, name):
            return getattr(self.data, name)

    mocked = MockTable(table, 2)
    result = print_table_preview(mocked, n_rows=2)

    assert "[...]" in result
    assert "20 rows x 2 columns" in result


def test_print_table_preview_scaling_integration():
    data = {"date": [date(2023, 1, 1)], "Mkt-RF": [5.238912]}
    table = pa.table(data)
    scaled_table = scale_to_decimal(table)

    class MockTable:
        def __init__(self, table, prec):
            self.data = table
            self._precision = prec
        def __getattr__(self, name):
            return getattr(self.data, name)

    mocked = MockTable(scaled_table, 4)
    result = print_table_preview(mocked)

    assert "0.0524" in result
    assert "5.2389" not in result

