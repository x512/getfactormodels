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
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from getfactormodels.models.base import FactorModel


class MockModel(FactorModel):
    @property
    def _frequencies(self) -> list[str]:
        return ['m', 'd', 'y']
    
    def _get_url(self) -> str:
        return "http://some-site.com/some-page"
    
    def _read(self, data: bytes) -> pd.DataFrame | None:
        return self._data


@pytest.fixture
def sample_model():
    data = {
        'date': pd.to_datetime(['2019-03-22', '2019-03-29']),
        'FACTOR_A': [-1.23, -4.5],
        'FACTOR_B': [67.8, 9.01],
        'FACTOR_C': [-12.3, 5.6]
    }
    df = pd.DataFrame(data).set_index('date')
    
    model = MockModel(frequency='m')
    model._data = df  # injecting data here.
    # should make sure client doesn't run ?
    return model


def test_extract_method(sample_model):
    """Tests for base class extract method."""
    result = sample_model.extract(['FACTOR_A', 'FACTOR_B'])
    assert list(result.columns) == ['FACTOR_A', 'FACTOR_B']
    assert len(result) == 2

    # Case-sensitive check (currently is case-sensitive)
    with pytest.raises(ValueError, match="not in model"):
        sample_model.extract(['factor_a'])

def test_extracting_the_index(sample_model):
    """Tests the index can't be extracted."""
    with pytest.raises(ValueError, match="not in model"):
        sample_model.extract('date')


def test_drop_method(sample_model, caplog):
    """Tests for base class drop method."""
    caplog.set_level(logging.INFO)

    # drop one col
    result = sample_model.drop('FACTOR_C')
    assert 'FACTOR_C' not in result.columns
    assert result.shape == (2, 2)

    # dropping an empty list
    result_none = sample_model.drop([])
    assert_frame_equal(result_none, sample_model.data)
    assert "method called with empty factor list" in caplog.text

    # dropping all columns (should fail)
    all_cols = list(sample_model.data.columns)
    with pytest.raises(ValueError, match="Can't drop all columns"):
        sample_model.drop(all_cols)

def test_dropping_the_index(sample_model):
    """Tests that the index can't be dropped."""
    # Attempting to drop the index 'date'
    with pytest.raises(ValueError, match="not in model"):
        sample_model.drop('date')


# base tests needed: _download_from_url: fails. 
# pa.Table given to _download
# CHeck for factors gets an empty datafrrame
#dont know how, but the 3 abstract methods need to be tested (use a model?)
