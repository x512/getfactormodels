#!/usr/bin/env python3
# getfactormodels: A Python package to retrieve financial factor model data.
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
from datetime import date
from types import SimpleNamespace
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.feather as feather
import pyarrow.ipc as ipc
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


@pytest.mark.parametrize(
    "cols, expected",
    [
        (["A", "B", "Mkt-RF", "RF", "date"], ["date", "Mkt-RF", "A", "B", "RF"]),
        (["RF", "B", "date"], ["date", "B", "RF"]),
        (["date", "Mkt-RF", "B"], ["date", "Mkt-RF", "B"]),
    ],
)
def test_rearrange_columns(cols, expected):
    table = pa.table({c: [1.0] for c in cols})
    result = rearrange_columns(table)
    assert result.column_names == expected


def test_validate_columns_errors():
    empty_table = pa.table({"a": []})
    with pytest.raises(RuntimeError, match="Table is empty."):
        _validate_columns(empty_table, ["a"])

    assert _validate_columns(pa.table({"a": [1]}), None) == []
    assert _validate_columns(pa.table({"a": [1]}), []) == []


def test_select_table_columns(sample_table):
    selected = select_table_columns(sample_table, ["sMb", "DATE"])
    assert selected.column_names == ["date", "SMB"]

    with pytest.raises(ValueError, match="at least one factor."):
        select_table_columns(sample_table, ["date"])

    with pytest.raises(ValueError, match="Columns not found"):
        select_table_columns(sample_table, ["non_existent"])


def test_filter_table_by_date(sample_table):
    filtered = filter_table_by_date(sample_table, start="2023-01-02", end=None)
    assert filtered.num_rows == 2

    filtered = filter_table_by_date(sample_table, start="2023-01-02", end="2023-01-02")
    assert filtered.num_rows == 1
    assert filtered.column("date")[0].as_py() == date(2023, 1, 2)

    assert filter_table_by_date(sample_table, None, None).num_rows == 3


def test_scale_to_decimal(sample_table):
    scaled = scale_to_decimal(sample_table)
    assert scaled.column("Mkt-RF")[0].as_py() == pytest.approx(0.011)


def test_round_to_precision():
    data = {"Mkt-RF": [1.1234567], "RF": [0.0123456]}
    table = pa.table(data)

    rounded = round_to_precision(table, precision=4)
    assert rounded.column("RF")[0].as_py() == pytest.approx(0.0123)
    assert rounded.column("Mkt-RF")[0].as_py() == pytest.approx(1.1235)


def test_format_for_preview_nans():
    assert _format_for_preview(None, "Mkt-RF") == "NaN"
    assert _format_for_preview(float("nan"), "SMB") == "NaN"
    assert _format_for_preview(1.23, "date") == "1.23"


def test_print_table_preview_empty(caplog):
    empty_table = pa.table({"date": [], "Mkt-RF": []})
    with caplog.at_level(logging.WARNING):
        result = print_table_preview(empty_table)
    assert result is None
    assert "Empty Table" in caplog.text


def test_print_table_preview_gap():
    data = {"date": [date(2023, 1, i + 1) for i in range(20)], "x": [1.0] * 20}
    table = pa.table(data)
    mocked = SimpleNamespace(
        data=table,
        _precision=2,
        **{k: getattr(table, k) for k in dir(table) if not k.startswith("_")},
    )

    result = print_table_preview(mocked, n_rows=2)
    assert "[...]" in result
    assert "20 rows x 2 columns" in result


def test_print_table_preview_scaling_integration():
    data = {"date": [date(2023, 1, 1)], "Mkt-RF": [5.238912]}
    table = pa.table(data)
    scaled_table = scale_to_decimal(table)
    mocked = SimpleNamespace(
        data=scaled_table,
        _precision=4,
        **{k: getattr(table, k) for k in dir(table) if not k.startswith("_")},
    )

    result = print_table_preview(mocked)
    assert "0.0524" in result
    assert "5.2389" not in result


# PyArrow 25.0.0
@pytest.mark.filterwarnings("error::DeprecationWarning")
def test_feather_ipc_roundtrip(tmp_path, sample_table):
    """Ensures saving and loading feather/IPC formats raises no deprecation warnings."""
    file_path = tmp_path / "test_model.feather"

    feather.write_feather(sample_table, file_path)
    read_table = feather.read_table(file_path)
    assert read_table.num_rows == sample_table.num_rows
    assert read_table.column_names == sample_table.column_names

    # Test IPC stream writing directly
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, sample_table.schema) as writer:
        writer.write_table(sample_table)
    buf = sink.getvalue()

    with ipc.open_stream(buf) as reader:
        ipc_table = reader.read_all()
    assert ipc_table.num_rows == sample_table.num_rows

# PyArrow 24
#
def test_csv_empty_batch_handling():
    """Validates that reading a CSV with an empty dataset yields empty table with correct columns."""
    csv_bytes = b"date,Mkt-RF,SMB,RF\n"
    table = pv.read_csv(io.BytesIO(csv_bytes))

    assert table.num_rows == 0
    assert table.column_names == ["date", "Mkt-RF", "SMB", "RF"]
