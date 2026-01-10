from datetime import date
import pyarrow as pa
import pytest
from getfactormodels.utils.date_utils import (
    _validate_input_date,
    offset_period_eom,
    parse_quarterly_dates,
    validate_date_range,
)


@pytest.mark.parametrize("input_val, is_end, expected", [
    ("2020", False, "2020-01-01"),
    ("2020", True, "2020-12-31"),
    ("2023-1", False, "2023-01-01"),
    ("2023-1-5", False, "2023-01-05"),
    (202206, True, "2022-06-30"),    # YYYYMM logic
    ("2022/03/31", False, "2022-03-31"),
])

def test_validate_input_date_formats(input_val, is_end, expected):
    assert _validate_input_date(input_val, is_end=is_end) == expected

def test_validate_input_date_none():
    assert _validate_input_date(None) is None

def test_validate_input_date_iso_and_is_end():
    assert _validate_input_date("2023-06", is_end=True) == "2023-06-30"
    assert _validate_input_date("202309", is_end=True) == "2023-09-30"

def test_validate_input_date_with_bad_date():
    with pytest.raises(ValueError, match="Invalid date"):
        _validate_input_date("2023-13-40")

def test_validate_input_date_invalid():
    with pytest.raises(ValueError, match="Invalid date"):
        _validate_input_date("not-a-date")

def test_validate_date_range_err(caplog):
    # Test start > end
    with pytest.raises(ValueError, match="is after end_date"):
        validate_date_range("2023-01-01", "2022-01-01")
    
    # Test start in future
    future_date = "2099-01-01"
    with pytest.raises(ValueError, match="future"):
        validate_date_range(future_date, None)

def test_validate_date_range_future_correction(capsys):
    future_date = "2099-12-31"
    today = date.today().isoformat()
    start, end = validate_date_range("2020-01-01", future_date)
    
    assert end == today

    # Check stderr
    captured = capsys.readouterr()
    assert "WARNING: End date is in the future" in captured.err

def test_validate_date_range_success():
    s, e = validate_date_range("2020-01-01", "2020-02-01")
    assert s == "2020-01-01"
    assert e == "2020-02-01"

def test_offset_period_eom():
    # YYYYMM strings
    data = {"date": ["202301", "202302"]}
    table = pa.table(data)
    
    # Offset to Monthly EOM
    result = offset_period_eom(table, "m")
    dates = result.column(0).to_pylist()
    
    assert dates[0] == date(2023, 1, 31)
    assert dates[1] == date(2023, 2, 28)

def test_parse_quarterly_dates_single_col():
    data = pa.table({"raw": ["20231", "20234"]})
    result = parse_quarterly_dates(data)
    result = offset_period_eom(result, frequency='q') # fix: it used to return offset.
    # Q1 to March 31, Q4 to Dec 31
    assert result.column("date")[0].as_py() == date(2023, 3, 31)
    assert result.column("date")[1].as_py() == date(2023, 12, 31)

def test_parse_quarterly_dates_empty_return():
    empty = pa.table({"year": [], "period": []})
    result = parse_quarterly_dates(empty)
    assert result.num_rows == 0
