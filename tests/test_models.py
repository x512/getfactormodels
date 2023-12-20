# -*- coding: utf-8 -*-
import argparse
import os
import unittest
from unittest.mock import patch
import pandas as pd
from pandas.testing import assert_frame_equal  # noqa
from getfactormodels import FactorExtractor
from getfactormodels.models.models import carhart_factors  # noqa; noqa: E501
from getfactormodels.models.models import (dhs_factors, ff_factors,
                                           icr_factors, liquidity_factors,
                                           mispricing_factors,
                                           q_classic_factors, q_factors)
from getfactormodels.utils import cli


class TestFactorModels(unittest.TestCase):
    def test_liquidity_factors(self):
        result = liquidity_factors(frequency='m', start_date='2010-01-01')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_mispricing_factors(self):
        result = mispricing_factors(end_date='2010-01-01')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_dhs_factors(self):
        result = dhs_factors(frequency='m', start_date='2010-01-01',
                             end_date='2010-01-01')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_icr_factors(self):
        result = icr_factors()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_carhart_factors(self):
        result = carhart_factors(frequency='m', end_date='2022-01-01')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_ff_with_4_factors(self):
        result = ff_factors(model="4", frequency='M', end_date='2022-01-01')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_carhart_and_4_factors(self):
        carhart = carhart_factors(frequency='M', end_date='2022-01-01')
        four = ff_factors(model="4", frequency='M', end_date='2022-01-01')
        assert_frame_equal(carhart, four)

    def test_q_factors(self):
        result = q_factors(frequency='M')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_ff_factors(self):
        result = ff_factors(model="6", frequency='D', start_date='2004-01-01',
                            end_date='2014-12-31')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_ff_factors_with_int(self):
        result = ff_factors(model=5, frequency='Y')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_mispricing_factors_with_invalid_freq(self):
        with self.assertRaises(ValueError):
            mispricing_factors(frequency='W')

    def test_icr_factors_with_invalid_freq(self):
        with self.assertRaises(ValueError):
            icr_factors(frequency='W')

    def test_liquidity_factors_with_invalid_freq(self):
        with self.assertRaises(ValueError):
            liquidity_factors(frequency='W')

    def test_dhs_factors_with_invalid_freq(self):
        with self.assertRaises(ValueError):
            dhs_factors(frequency='W')

    def q_classic_factors_with_invalid_freq(self):
        with self.assertRaises(ValueError):
            q_classic_factors(frequency='x')

    def test_q_classic(self):
        result = q_classic_factors(frequency='Q')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    def test_ff_factors_weekly(self):
        result = ff_factors(model="3", frequency='W', start_date='2004-01-01',
                            end_date='2014-12-31')
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)


class TestFactorExtractor(unittest.TestCase):
    def setUp(self):
        self.extractor = FactorExtractor()

    def test_no_rf(self):
        self.extractor.no_rf()
        self.assertTrue(self.extractor._no_rf)

    def test_to_file_with_no_data(self):
        with self.assertRaises(ValueError):
            self.extractor.to_file('output.csv')

    def test_to_file_with_data(self):
        self.extractor.df = pd.DataFrame({'Factor1': [1, 2, 3],
                                          'Factor2': [4, 5, 6]})
        self.extractor.to_file('output.csv')
        self.assertTrue(os.path.exists('output.csv'))
        os.remove('output.csv')

    def tearDown(self):
        if os.path.exists('output.csv'):
            os.remove('output.csv')


class TestKeysTestCLI(unittest.TestCase):
    # ruff: noqa: D100, D101, D102, D103
    def setUp(self):
        self.extractor = FactorExtractor()

    @patch('argparse.ArgumentParser.parse_args')
    def test_parse_args(self, mock_args):
        mock_args.return_value = argparse.Namespace(model='3', freq='M',
                                                    start='1961-01-01',
                                                    end='1990-12-31')

        args = cli.parse_args()
        self.assertEqual(args.model, '3')
        self.assertEqual(args.freq, 'M')
        self.assertEqual(args.start, '1961-01-01')
        self.assertEqual(args.end, '1990-12-31')


if __name__ == '__main__':
    unittest.main()
# [TODO]: Cov/Redo tests, these are just simple tests to get started.
# [TODO] - every key appended with _factor or _factors should match the same
# key.
#        - Test: to be safe, q or q_classic regex shouldn't match (haven't yet
# 'quarterly', 'Q4', 'Qtr.', 'qtrly', q4 (which should match q_classic), and m
# shouldn't match mispricing
#        - Test all URLs for a 200 response. verify true
