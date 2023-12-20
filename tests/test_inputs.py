# -*- coding: utf-8 -*-
import unittest
import pandas as pd
from getfactormodels import get_factors
from getfactormodels.__main__ import FactorExtractor


class TestFactorExtractorBasic(unittest.TestCase):
    def test_no_rf(self):
        fe = FactorExtractor()
        fe.no_rf()
        self.assertTrue(fe._no_rf)

    def test_drop_rf(self):
        fe = FactorExtractor()
        df = pd.DataFrame({'A': [1, 2, 3], 'RF': [0.1, 0.2, 0.3]})
        result = fe.drop_rf(df)
        self.assertNotIn('RF', result.columns)


class TestFactorExtractorTwo(unittest.TestCase):
    def test_date_input_format(self):
        test_cases = [  # YYYY-MM-DD both start and end
                    ('1965-01-01', '1985-12-31', '1965-01-01', '1985-12-31'),
                    # DD-MM-YYYY both start and end
                    ('01-01-1965', '31-12-1985', '1965-01-01', '1985-12-31'),
                    # YYYY-MM-DD start, DD-MM-YYYY end
                    ('1965-01-01', '31-12-1985', '1965-01-01', '1985-12-31'),
                    # DD-MM-YYYY start, YYYY-MM-DD end
                    ('01-01-1965', '1985-12-31', '1965-01-01', '1985-12-31'),
                    # YYYY-MM-DD start, None end
                    ('1965-01-01', None, '1965-01-01', None),
                    # DD-MM-YYYY start, None end
                    ('01-01-1965', None, '1965-01-01', None),
                    # None start, YYYY-MM-DD end
                    (None, '1985-12-31', None, '1985-12-31'),
                    # None start, DD-MM-YYYY end
                    (None, '31-12-1985', None, '1985-12-31'),
                    # None start, None end
                    (None, None, None, None),
                    # d-mm-yyyy
                    ('1-1-1965', '31-1-1985', '1965-01-01', '1985-01-31'),]

        for i, (start_date, end_date, expected_start, expected_end) in enumerate(test_cases):  # noqa
            with self.subTest(i=i):
                fe = FactorExtractor(start_date=start_date, end_date=end_date)
                self.assertEqual(fe.start_date, expected_start)
                self.assertEqual(fe.end_date, expected_end)

    def test_date_input_format_raises(self):
        bad_date = '41-14-1965'
        with self.assertRaises(ValueError):
            FactorExtractor(start_date=bad_date)

    def test_wrong_model_key_to_get_factors(self):
        # Class method.                             [TODO: rename one of these]
        fe = FactorExtractor(model='not a model.')
        with self.assertRaises(ValueError):
            fe.get_factors()

        # The get_factors (function) should raise the same error
        with self.assertRaises(ValueError):
            get_factors(model='not a model.')


if __name__ == '__main__':
    unittest.main()
