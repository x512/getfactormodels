# -*- coding: utf-8 -*-
# ruff: noqa: SIM117
import datetime
import os
import unittest
import pandas as pd
from getfactormodels import FactorExtractor
from getfactormodels.utils.utils import (_get_model_key, _rearrange_cols,
                                         _save_to_file, _slice_dates,
                                         _validate_date)


class TestRearrangeCols(unittest.TestCase):
    def setUp(self):
        self.extractor = FactorExtractor()
        self.index = pd.date_range(start='1/1/2022', periods=5)
        self.data = pd.DataFrame(
            {"A": [1.55, -2.434, 300.4, 0.555555355, -0.7],
             "B": [4.0, 50.25, 600.7, -1.2, 2.3],
             "Mkt-RF": [7.1234, -80.26667, 9.35, -2.3, 3.4],
             "RF": [10.5, -110.6, 1200.7, -3.4, 4.5]}, index=self.index)

    def test_rearrange_cols(self):
        self._test_rearrange_cols(self.data, ["Mkt-RF", "A", "B", "RF"])

    def _test_rearrange_cols(self, data, expected_cols):
        result = _rearrange_cols(data)
        self.assertEqual(list(result.columns), expected_cols)

    def test_rearrange_cols_without_rf(self):
        data = self.data.drop(columns=['RF'])
        self._test_rearrange_cols(data, ["Mkt-RF", "A", "B"])

    def test_rearrange_cols_without_mkt_rf(self):
        data = self.data.drop(columns=['Mkt-RF'])
        self._test_rearrange_cols(data, ["A", "B", "RF"])

    def test_rearrange_cols_with_empty_df(self):
        data = pd.DataFrame()
        self._test_rearrange_cols(data, [])

    def test_with_series(self):
        series = pd.Series([1, 2, 3], name='Mkt-RF')
        result = _rearrange_cols(series)
        pd.testing.assert_series_equal(result, series)

    def simple_test_rearrange_cols(self):
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
        result = _rearrange_cols(df)
        pd.testing.assert_frame_equal(result, expected)

    def test_validate_date(self):
        self.assertEqual(_validate_date('2022-01-01'), '2022-01-01')
        self.assertEqual(_validate_date('01-01-2022'), '2022-01-01')
        self.assertEqual(_validate_date('20220101'), '2022-01-01')
        self.assertEqual(_validate_date(None), None)

        # Test with non-date string
        with self.assertRaises(ValueError):
            _validate_date('Hi, not a date...')

        # Test with date object
        with self.assertRaises(TypeError):
            _validate_date(datetime.date(2022, 1, 1))

        # Test with datetime object
        with self.assertRaises(TypeError):
            _validate_date(datetime.datetime(2022, 1, 1))  # noqa: DTZ001

        # Test with other data types
        with self.assertRaises(TypeError):
            _validate_date(123)
        with self.assertRaises(TypeError):
            _validate_date(-13.85)

    def test_slice_dates(self):
        sliced_data = _slice_dates(self.data, '2022-01-01', '2022-01-02')
        self.assertEqual(len(sliced_data), 2)
        self.assertEqual(sliced_data.index[1], pd.to_datetime('2022-01-02'))


class TestGetModelKey(unittest.TestCase):
    def test_model_keys(self):
        test_cases = [
            ('ff1993', '3'),
            ('ff5', '5'),
            ('ff4', '4'),
            ('ff2015', '5'),
            ('ff6', '6'),
            ('ff2018', '6'),
            ('6', '6'),
            ('bs', 'barillas_shanken'),
            ('car', '4'),
            ('carhart', '4'),
            ('carhart1997', '4'),
            ('hkm', 'icr'),
            ('hml_d', 'hml_devil'),
            ('hmld', 'hml_devil'),
            ('hmldevil', 'hml_devil'),
            ('ICR', 'icr'),
            ('illiq', 'liquidity'),
            ('liQ', 'liquidity'),
            ('LIQUiDity', 'liquidity'),
            ('m4', 'mispricing'),
            ('ps', 'liquidity'),
            ('hmxz', 'q'),
            ('q4', 'q_classic'),
            ('q5', 'q'),
            ('qclassic', 'q_classic'),
            ('classic_q', 'q_classic'),
            ('sy', 'mispricing'),
            ('not_a_model', ValueError),
            (8, ValueError),  # make TypeError
            (12.25, ValueError), ]  # Same here

        for model, expected_key in test_cases:
            with self.subTest(model=model):
                if expected_key in [ValueError, TypeError]:
                    with self.assertRaises(expected_key):
                        _get_model_key(model)
                else:
                    model_key = _get_model_key(model)
                    self.assertEqual(model_key, expected_key)

    def test_nono(self):
        nonono = ['m', 'M', 'W', 'w', 'D', 'd', 'qtr', '2', '7', 'i']
        for model in nonono:
            with self.subTest(model=model):
                with self.assertRaises(ValueError):
                    _get_model_key(model)


class TestSaveToFile(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({'Factor1': [1, 2, 3], 'Factor2': [4, 5, 6]})
        self.series = self.df['Factor1']
        self.dict = {'Factor1': [1, 2, 3], 'Factor2': [4, 5, 6]}
        self.files = ['output.csv', 's.csv', 'output.md', 'output.txt',
                      'output.pkl', 'output.xlsx']

    def tearDown(self):
        for file in self.files:
            if os.path.exists(file):
                os.remove(file)

    def test_save_dict_raises_error(self):
        with self.assertRaises(ValueError):
            _save_to_file(self.dict, 'bad_file.csv')

    def test_save_dataframe(self):
        _save_to_file(self.df, 'output.csv')
        self.assertTrue(os.path.exists('output.csv'))
        result_df = pd.read_csv('output.csv', index_col=0)
        pd.testing.assert_frame_equal(result_df, self.df)

    def test_save_series(self):
        _save_to_file(self.series, 's.csv')
        self.assertTrue(os.path.exists('s.csv'))
        result_series = pd.read_csv('s.csv', index_col=0)
        if len(result_series.columns) == 1:
            result_series = result_series.iloc[:, 0]
        pd.testing.assert_series_equal(result_series, self.series)

    def test_save_to_different_formats(self):
        formats = ['md', 'txt', 'pkl', 'xlsx']
        for format in formats:
            filename = f'output.{format}'
            _save_to_file(self.df, filename)
            self.assertTrue(os.path.exists(filename))


if __name__ == '__main__':
    unittest.main()
