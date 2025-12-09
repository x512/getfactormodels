import datetime
import unittest
from unittest.mock import mock_open, patch
import pandas as pd
from getfactormodels import (CarhartFactors, FamaFrenchFactors,
                             LiquidityFactors, QFactors, get_factors)
from getfactormodels.utils import cli
from getfactormodels.utils.utils import (_pd_rearrange_cols, _slice_dates,
                                         _validate_date)


class TestFactorModelClasses(unittest.TestCase):
    def test_q_classic(self):
        """Test QFactors with classic option."""
        result = QFactors(frequency='q', classic=True).download()
        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

        qresult = QFactors(frequency='q').download()
        self.assertIsNotNone(qresult)
        self.assertIsInstance(qresult, pd.DataFrame)

        # q5 col =/= q classic cols.
        self.assertNotEqual(set(qresult.columns), set(result.columns))

    def test_weekly_freq(self):
        """Test models with weekly frequency."""
        wk_data = FamaFrenchFactors(model='3', frequency='w').download()
        self.assertIsNotNone(wk_data)
        self.assertIsInstance(wk_data, pd.DataFrame)

        with self.assertRaises(ValueError):
            LiquidityFactors(frequency='w', output_file='test.csv')

    def test_carhart_and_4_factors(self):
        """Test that CarhartFactors and FamaFrenchFactors model 4 are equivalent."""
        carhart = CarhartFactors(frequency='m', end_date='2022-01-01').download()
        four_factor = FamaFrenchFactors(model=4, frequency='m', end_date='2022-01-01').download()

        self.assertIsNotNone(carhart)
        self.assertIsNotNone(four_factor)
        self.assertIsInstance(carhart, pd.DataFrame)
        self.assertIsInstance(four_factor, pd.DataFrame)

        pd.testing.assert_frame_equal(carhart, four_factor)

class TestGetFactorsFunc(unittest.TestCase):
    def test_weekly_freq_func(self):
        # Only ff 3 and q5 have weekly.
        wk_ff_data = get_factors(model='3', frequency='w')
        self.assertIsNotNone(wk_ff_data)
        self.assertIsInstance(wk_ff_data, pd.DataFrame)

        wk_q_data = get_factors(model='q', frequency='w', start_date='2009-01-01', end_date='2009-02-01')
        self.assertIsNotNone(wk_q_data)
        self.assertIsInstance(wk_q_data, pd.DataFrame)

    def get_dhs_factors(self):
        data = get_factors(model='dhs', frequency='m', start_date='2000-01-01', end_date='2000-01-09')
        self.assertIsNotNone(data)
        self.assertIsInstance(data, pd.DataFrame)

    def get_icr_factors(self):
        data = get_factors(model='icr', frequency='q', start_date='2000-01-01', end_date='2004-07-09')
        self.assertIsNotNone(data)
        self.assertIsInstance(data, pd.DataFrame)

    def get_liq_factors(self):
        data = get_factors(model='liq', frequency='d', start_date='2015-05-25', end_date='2015-07-09')
        self.assertIsNotNone(data)
        self.assertIsInstance(data, pd.DataFrame)

    def get_ff6_factors(self):
        data = get_factors(model='6', frequency='y', start_date='2010-01-25', end_date='2012-11-09')
        self.assertIsNotNone(data)
        self.assertIsInstance(data, pd.DataFrame)
        class_data = FamaFrenchFactors(model='6', frequency='y', start_date='2010-01-25', end_date='2012-11-09')
        self.assertEqual(data, class_data)

#class TestCliModelParams(unittest.TestCase):
#

class TestDateFunctions(unittest.TestCase):
    def test_validate_date_various_formats(self):
        """Test _validate_date converts various date formats to YYYY-MM-DD."""
        test_cases = [
            ("2023-06-15", "2023-06-15"),
            ("20230615", "2023-06-15"),
            ("2023", "2023-01-01"),
            ("2023-06", "2023-06-01"),
            (pd.Timestamp("2023-06-15"), "2023-06-15"),
            (datetime.datetime(2023, 6, 15), "2023-06-15"),
            ("2023/06/15", "2023-06-15"),
            (None, None),
        ]

        for date_input, expected in test_cases:
            with self.subTest(input=date_input, expected=expected):
                result = _validate_date(date_input)
                self.assertEqual(result, expected)

    def test_validate_date_invalid_formats(self):
        """Test _validate_date raises errors for invalid formats."""
        invalid_cases = [
            "not-a-date",
            "2023-13-01",
            "2023-06-32",
            "202306150",
            {"not": "a date"},
        ]

        for invalid_date in invalid_cases:
            with self.subTest(input=invalid_date):
                with self.assertRaises((ValueError, TypeError)):
                    _validate_date(invalid_date)

    def test_slice_dates_simple(self):
        """Test _slice_dates correctly slices a DataFrame."""
        dates = pd.date_range('2023-01-01', periods=10, freq='D')
        df = pd.DataFrame({'value': range(10)}, index=dates)

        sliced = _slice_dates(df, start_date='2023-01-03', end_date='2023-01-07')

        self.assertEqual(len(sliced), 5)
        self.assertEqual(sliced.index[0], pd.Timestamp('2023-01-03'))
        self.assertEqual(sliced.index[-1], pd.Timestamp('2023-01-07'))
        self.assertEqual(sliced['value'].iloc[0], 2)
        self.assertEqual(sliced['value'].iloc[-1], 6)

    def test_slice_dates_partial(self):
        """Test _slice_dates with only start or only end date."""
        dates = pd.date_range('2023-01-01', periods=10, freq='D')
        df = pd.DataFrame({'value': range(10)}, index=dates)

        sliced_start = _slice_dates(df, start_date='2023-01-05')
        self.assertEqual(len(sliced_start), 6)
        self.assertEqual(sliced_start.index[0], pd.Timestamp('2023-01-05'))
        self.assertEqual(sliced_start.index[-1], pd.Timestamp('2023-01-10'))

        sliced_end = _slice_dates(df, end_date='2023-01-04')
        self.assertEqual(len(sliced_end), 4)
        self.assertEqual(sliced_end.index[0], pd.Timestamp('2023-01-01'))
        self.assertEqual(sliced_end.index[-1], pd.Timestamp('2023-01-04'))


class TestRearrangeCols(unittest.TestCase):
    def setUp(self):
        self.index = pd.date_range(start='1/1/2022', periods=5)
        self.data = pd.DataFrame(
            {"A": [331.55, -2.434, 300.4, 0.555555355, -0.7],
             "B": [45.0, 50.25, 632400.7, -1.2, 2.3],
             "Mkt-RF": [7.1234, -80.26667, 9.35, -2.3, 3.4],
             "RF": [10.5, -110.6, 1200.7, -3.4, 4.5]}, index=self.index)

    def test_rearrange_cols(self):
        result = _pd_rearrange_cols(self.data)
        self.assertEqual(list(result.columns), ["Mkt-RF", "A", "B", "RF"])

    def test_rearrange_cols_without_rf(self):
        data = self.data.drop(columns=['RF'])
        result = _pd_rearrange_cols(data)
        self.assertEqual(list(result.columns), ["Mkt-RF", "A", "B"])

    def test_rearrange_cols_without_mkt_rf(self):
        data = self.data.drop(columns=['Mkt-RF'])
        result = _pd_rearrange_cols(data)
        self.assertEqual(list(result.columns), ["A", "B", "RF"])

    def test_rearrange_cols_with_empty_df(self):
        data = pd.DataFrame()
        result = _pd_rearrange_cols(data)
        self.assertEqual(list(result.columns), [])

    def test_with_series(self):
        series = pd.Series([1, 2, 3], name='Mkt-RF')
        result = _pd_rearrange_cols(series)
        pd.testing.assert_series_equal(result, series)

    def test_simple_rearrange_cols(self):
        df = pd.DataFrame({
            'A': [1, 2, 3],
            'Mkt-RF': [4, 5, 6],
            'RF': [7, 8, 9]
        })
        expected = pd.DataFrame({
            'Mkt-RF': [4, 5, 6],
            'A': [1, 2, 3],
            'RF': [7, 8, 9]
        })
        result = _pd_rearrange_cols(df)
        pd.testing.assert_frame_equal(result, expected)


if __name__ == '__main__':
    unittest.main()
