# -*- coding: utf-8 -*-
import unittest
import pandas as pd
import requests
from getfactormodels.models.ff_models import (_ff_construct_url, _ff_get_mom,
                                              _ff_process_data,
                                              _ff_read_csv_from_zip,
                                              _get_ff_factors)
from getfactormodels.models.models import ff_factors


class TestFFPublicFunction(unittest.TestCase):
    # Invalid freq, invalid model, valid model (if int)

    def test_ff_factors_invalid_freq(self):
        with self.assertRaises(ValueError):
            ff_factors(model="3", frequency="T")

    def test_ff_factors_invalid_model(self):
        with self.assertRaises(ValueError):
            ff_factors(model="7", frequency="M")

    def test_ff_model_param_with_int(self):
        factors = ff_factors(model=3, frequency="M")
        self.assertIsInstance(factors, pd.DataFrame)
        self.assertIsInstance(factors.index, pd.DatetimeIndex)
        self.assertTrue(factors.index.is_monotonic_increasing)

    def test_ff_model_param_with_invalid_int(self):
        with self.assertRaises(ValueError):
            ff_factors(model=7, frequency="Y")
        with self.assertRaises(ValueError):
            ff_factors(model=2, frequency="Y")


class TestFFPrivates(unittest.TestCase):

    def test_ff_get_mom(self):
        mom = _ff_get_mom(frequency="M")
        self.assertIsNotNone(mom)
        # self.assertTrue(mom.index.is_monotonic_increasing) it isnt? NaNs??
        # self.assertEqual(len(mom.index), len(mom.index.unique()))

    def test_get_ff_factors(self):
        factors = _get_ff_factors(model="3", frequency="M")
        self.assertIsNotNone(factors)
        self.assertIsInstance(factors.index, pd.DatetimeIndex)
        self.assertTrue(factors.index.is_monotonic_increasing)
        self.assertIsInstance(factors, pd.DataFrame)

    def test_ff_construct_url(self):
        base_url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/"
        ff_url = f"{base_url}ftp/F-F_Research_Data_Factors_CSV.zip"

        url = _ff_construct_url(model="3", frequency="M")
        self.assertEqual(url, ff_url)

    def test_ff_url_for_response(self):
        url = _ff_construct_url(model="3", frequency="M")
        response = requests.get(url, timeout=8)
        self.assertEqual(response.status_code, 200)

    # should get a ValueError with invalid freq, e.g. "T"
    def test_get_freq_with_invalid_value(self):
        with self.assertRaises(ValueError):
            _get_ff_factors(model="3", frequency="T")


if __name__ == '__main__':
    unittest.main()
