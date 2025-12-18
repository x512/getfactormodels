from unittest.mock import patch
import pandas as pd
import pytest
from getfactormodels.models.icr import ICRFactors
from getfactormodels.models.q_factors import QFactors


@pytest.fixture
def model():
    return ICRFactors(frequency='q')

# Test for parsing quarterly dates in ICR
def test_icr_quarterly_date_parsing(model):
    csv_content = (
        "yyyyq,intermediary_capital_ratio,intermediary_capital_risk_factor,"
            "intermediary_value_weighted_investment_return,intermediary_leverage_ratio_squared\n"
            "20251,0.12,-3.45,6.78,-9.0\n"
            "20252,0.13,0.6,0.3,1.6\n"
            "20253,0.11,-0.45,,-3.4"
    ).encode('utf-8')

    # ICR's _read
    df = model._read(csv_content)

    # index is datetime
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index[0] == pd.Timestamp("2025-03-31")
    assert df.index[1] == pd.Timestamp("2025-06-30")
    # cols renamed
    assert "IC_RATIO" in df.columns

# and daily and monthly for coverage:
def test_icr_daily_date_parsing():
    model = ICRFactors(frequency='d')
    csv_content = (
        "yyyymmdd,intermediary_capital_ratio,intermediary_capital_risk_factor,"
            "intermediary_value_weighted_investment_return,intermediary_leverage_ratio_squared\n"
            "20230131,0.10,0.01,0.01,1.1\n"
            "20230712,0.11,0.02,0.02,1.2"
    ).encode('utf-8')

    df = model._read(csv_content)

    assert df.index[1] == pd.Timestamp("2023-07-12")
    assert "IC_RISK_FACTOR" in df.columns

def test_icr_month_date_parsing():
    model = ICRFactors(frequency='m')
    csv_content = (
        "yyyymm,intermediary_capital_ratio,intermediary_capital_risk_factor,"
            "intermediary_value_weighted_investment_return,intermediary_leverage_ratio_squared\n"
            "202301,0.10,0.01,0.01,1.1\n"
            "199912,0.11,0.02,0.02,1.2"
    ).encode('utf-8')

    df = model._read(csv_content)

    assert df.index[1] == pd.Timestamp("1999-12-31")
    assert "INT_VW_ROI" in df.columns


# Tests that the schema enforces types
def test_icr_schema_type_enforcement(model):
    csv_content = (
        "yyyyq,intermediary_capital_ratio,intermediary_capital_risk_factor,"
            "intermediary_value_weighted_investment_return,intermediary_leverage_ratio_squared\n"
            "20251,BAD_DATA,0.05,-0.02,1.5" # returning bad data
    ).encode('utf-8')

    with pytest.raises(ValueError, match="Error reading csv"):
        model._read(csv_content)

def test_icr_wrong_column_names(model):
    csv_content = (
        "date_col,icr,factor,roi,lev_sq\n"
            "20251,0.12,0.05,0.02,1.5"
    ).encode('utf-8')

    # pv.ConvertOptions(column_types=SCHEMA) means it should fail immediately.
    with pytest.raises(ValueError, match="Error reading csv"):
        model._read(csv_content)

def test_icr_url():
    model_m = ICRFactors(frequency='m')
    model_d = ICRFactors(frequency='d')

    url_m = model_m._get_url()
    url_d = model_d._get_url()

    assert "monthly" in url_m
    assert "daily" in url_d
    assert "2025/07" in url_m # hardcoded still

# Test rename logic
def test_icr_read_incorrect_col_names():
    model = ICRFactors(frequency='m')
    # Missing cols (1 returned)
    csv_content = "yyyymm,intermediary_capital_ratio\n202301,0.10".encode('utf-8')

    # SCHEMA won't match the CSV
    with pytest.raises(ValueError, match="Error reading csv"):
        model._read(csv_content)

# testing a empty csv: should return df with correct cols
def test_empty_csv_with_headers(model):
    csv_content = (
        "yyyyq,intermediary_capital_ratio,intermediary_capital_risk_factor,"
            "intermediary_value_weighted_investment_return,intermediary_leverage_ratio_squared\n"
    ).encode('utf-8')

    with patch("getfactormodels.utils.utils._process", side_effect=lambda df, *args, **kwargs: df):

        df = model._read(csv_content)
        assert df.empty
        assert list(df.columns) == ["IC_RATIO", "IC_RISK_FACTOR",
                                    "INT_VW_ROI", "INT_LEV_RATIO_SQ"]

# Qfactor tests
@pytest.fixture
def q_model():
    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        yield QFactors(frequency='m')

# qtr date parsing: checks date parsing works, not getting eg 1967093031!
def test_q_quarterly_date_parsing():
    model = QFactors(frequency='q')
    csv_content = (
        "year,quarter,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "2023,1,0.1,0.5,0.2,0.1,0.3,0.4\n"
        "2023,4,0.1,0.6,0.2,0.1,0.3,0.4"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    assert df.index[0] == pd.Timestamp("2023-03-31")
    assert df.index[1] == pd.Timestamp("2023-12-31")
    assert "MKT-RF" in df.columns

# 'm' date parsing: tests pd.MonthEnd offset works
def test_q_monthly_date_parsing():
    model = QFactors(frequency='m')
    csv_content = (
        "year,month,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "2023,1,0.1,0.5,0.2,0.1,0.3,0.4\n"
        "2023,2,0.1,0.6,0.2,0.1,0.3,0.4"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    assert df.index[0] == pd.Timestamp("2023-01-31")
    assert df.index[1] == pd.Timestamp("2023-02-28")
    assert "R_IA" in df.columns

# 'd'/'w'/'w2w' date parsing: drops source 'date' and replaces with timestamped 'date'
def test_q_daily_date_parsing():
    model = QFactors(frequency='d')
    csv_content = (
        "date,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "20230101,0.1,0.5,0.2,0.1,0.3,0.4"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    assert df.index[0] == pd.Timestamp("2023-01-01")
    assert "RF" in df.columns

def test_q_yearly_date_parsing():
    model = QFactors(frequency='d')
    csv_content = (
        "date,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "2023,0.1,0.5,0.2,0.1,0.3,0.4"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    assert df.index[0] == pd.Timestamp("2023-01-01")
    assert "RF" in df.columns


# tests the 'classic=' toggle for q-factors (dropping the r_eg col) 
def test_q_classic_return():
    model = QFactors(frequency='m', classic=True)
    csv_content = (
        "year,month,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "2023,1,0.1,0.5,0.2,0.1,0.3,0.4"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    # R_EG should be gone
    assert "R_EG" not in df.columns
    assert "R_ROE" in df.columns

# Decimalization (* 0.01)
def test_q_decimalization():
    model = QFactors(frequency='m')
    csv_content = (
        "year,month,r_f,r_mkt,r_me,r_ia,r_roe,r_eg\n"
        "2023,1,1.0,5.0,2.0,1.0,3.0,4.0"
    ).encode('utf-8')

    with patch("getfactormodels.models.q_factors._process", side_effect=lambda df, *args, **kwargs: df):
        df = model._read(csv_content)
    
    assert df["RF"].iloc[0] == 0.01
    assert df["MKT-RF"].iloc[0] == 0.05

def test_q_frequency_setter_wipe():
    model = QFactors(frequency='m')
    model._data = pd.DataFrame([1, 2, 3]) # pretend its data...
    model.frequency = 'd' # should trigger setter
    assert model._data is None # which should clear _data


# for coverage ln 82-92
def test_qfactors_url():
    q_q = QFactors(frequency='q')
    q_w2w = QFactors(frequency='w2w') #unique for q factors

    url_q = q_q._get_url()
    url_w2w = q_w2w._get_url()

    assert "weekly_w2w" in url_w2w
    assert "quarterly" in url_q
    assert "-q.org/uploads/1/2/2/6/12" in url_w2w # contains this random str from the base url...

# 94%. Needs: 71, 109-110, 132
# year freq (71, 132)
# 132 tests if 1231 is added to yyyy with pc.binary_join_element_wise; 71 is the elif self.frequency == "y" check in the schema 


# read error (raise exception -- make pyarrow?)


