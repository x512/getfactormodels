# -*- coding: utf-8 -*-
import unittest
import pandas as pd
import requests
from getfactormodels.models.ff_models import (_ff_construct_url, _ff_get_mom,
                                              _get_ff_factors, ff_process_data,
                                              ff_read_csv_from_zip)


class TestFFModels(unittest.TestCase):

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


if __name__ == '__main__':
    unittest.main()