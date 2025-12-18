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
from datetime import datetime
import pandas as pd
import pytest
from getfactormodels.utils.utils import (_get_model_key, _pd_rearrange_cols,
                                         _slice_dates, _validate_date)


# Test date input
@pytest.mark.parametrize("date_input, expected", [
    ("2023-06-15", "2023-06-15"), 
    ("20230615", "2023-06-15"),
    ("2023", "2023-01-01"), 
    ("2023-06", "2023-06-01"),
    (pd.Timestamp("2023-06-15"), "2023-06-15"),
    (datetime(2023, 6, 15), "2023-06-15"), (None, None),
])


def test_validate_date(date_input, expected):
    assert _validate_date(date_input) == expected

@pytest.mark.parametrize("invalid", ["not-a-date", "2023-13-01", "202306150", {"a": 1}])
def test_validate_date_errors(invalid):
    with pytest.raises((ValueError, TypeError)):
        _validate_date(invalid)

# Test parse dates
#

#_slice dates check
def test_slice_dates():
    df = pd.DataFrame({'v': range(10)}, index=pd.date_range('2023-01-01', periods=10))

    assert len(_slice_dates(df, '2023-01-03', '2023-01-07')) == 5
    assert _slice_dates(df, start_date='2023-01-09').index[0] == pd.Timestamp('2023-01-09')
    assert len(_slice_dates(df, end_date='2023-01-02')) == 2


# _pd_rearrange cols check
@pytest.mark.parametrize("cols, expected", [
    (["A", "B", "Mkt-RF", "RF"], ["Mkt-RF", "A", "B", "RF"]),
    (["RF", "B", "A"], ["B", "A", "RF"]),
    (["Mkt-RF", "B"], ["Mkt-RF", "B"]),
    (["A", "RF", "B"], ["A", "B", "RF"]),
    (["RF", "B", "Mkt-RF", "A" ], ["Mkt-RF", "B", "A", "RF"]),
    ([], []),
])

def test_rearrange_cols(cols, expected):
    df = pd.DataFrame(columns=cols)
    assert list(_pd_rearrange_cols(df).columns) == expected

def test_rearrange_series():
    s = pd.Series([1], name='Mkt-RF')
    pd.testing.assert_series_equal(_pd_rearrange_cols(s), s)


#utils/model_utils.py ? in base? But the regex
# REGEX, MODEL KEYS

# testing the insane regex
@pytest.mark.parametrize("model_input, expected", [
    ('3', '3'),
    (4, "4"),                      # FFFactors accept int or str 
    ('5', '5'),
    ('6', '6'),
    ('ff3', '3'),
    ('ff4', '4'),
    ('car', '4'), 
    ('carhart', '4'),
    ('ff5', '5'),
    ('ff6', '6'), 
    ('ff1993', '3'), 
    ('ff2015', '5'),
    ('ff2018', '6'), 
    ('bs', 'BarillasShanken'), 
    ('hkm', 'ICR'), ('icr', 'ICR'), ('iCR', 'ICR'),
    ('hml_d', 'HMLDevil'), ('HMLD', 'HMLDevil'), ('hmldevil', 'HMLDevil'),  
    ('ps', 'Liquidity'),  #pastor-stambaugh
    ('liq', 'Liquidity'), 
    ('LIQUIDity', 'Liquidity'),
    ('mis', 'Mispricing'),
    ('sy', 'Mispricing'), #stambaugh-yuan
    ('m4', 'Mispricing'), 
    ('hmxz', 'Q'),
    ('q5', 'Q'), 
    ('Dhs', "DHS"),
    ("behav", "DHS"),
    ("behaviour", "DHS"),
    ('qclassic', 'Qclassic'),
    ('classic_q', 'Qclassic'), 
    ('q4', 'Qclassic'), 
])

def test_get_model_key_success(model_input, expected):
    """Verify correct normalization of model aliases."""
    assert _get_model_key(model_input) == expected

@pytest.mark.parametrize("invalid_input, error_type", [
    ('not_a_model', ValueError),
    (8, ValueError),    
    ("12.25", ValueError),
    ([], ValueError),
])

def test_get_model_key_errors(invalid_input, error_type):
    """Verify that invalid models or types raise appropriate errors."""
    with pytest.raises(error_type):
        _get_model_key(invalid_input)

# testing prepare filepath, save to file 
# filename, default ext 
# with ext: .pkl, .csv, .txt 
# full path: "~/dir/filename"
# with ext.
# dir only. "~/dir/dir/"
