# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import pytest
import pyarrow as pa
from unittest.mock import patch

from getfactormodels.models.base import (
    Base,
    FactorModel,
    PortfolioBase,
    Anomalies,
    Composite,
    Collection,
    RegionMixin,
)


class DummyModel(FactorModel):
    def __init__(self, **kwargs):
        super().__init__(model="ff3", **kwargs)

    @property
    def url(self) -> str:
        return "https://example.com/data.zip"

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([("date", pa.date32()), ("Mkt-RF", pa.float64()), ("SMB", pa.float64())])

    def _get_data(self, client=None) -> pa.Table:
        """Bypass real network calls for testing."""
        return self._read(b"")

    def _read(self, data: bytes) -> pa.Table:
        return pa.Table.from_pydict({
            "date": ["2020-01-01", "2021-01-01", "2022-01-01"],
            "Mkt-RF": [1.0, 2.0, 3.0],
            "SMB": [0.5, 0.6, 0.7],
        }).cast(self.schema)


class DummyRegionalModel(DummyModel, RegionMixin):
    @property
    def regions(self) -> list[str]:
        return ["us", "gbr"]


def test_base_init_and_properties():
    """Test initialization, frequency validation, name lookup, and repr."""
    model = DummyModel(frequency="m", start_date="2020", end_date="2022")
    assert model.frequency == "m"
    assert model.start_date == "2020-01-01"
    assert model.end_date == "2022-12-31"
    assert "Fama-French 3-Factor Model" in model.name
    assert "DummyModel" in repr(model)

    # bad frequency
    with pytest.raises(ValueError, match="Invalid frequency"):
        model.frequency = "invalid"

    # None frequency
    model.frequency = None
    assert model.frequency is None

    # bad kwargs
    with pytest.raises(TypeError, match="Unexpected keyword argument"):
        DummyModel(bad_arg=123)


def test_base_extract():
    """Test extracting single or multiple factor columns."""
    model1 = DummyModel()
    model1.extract("Mkt-RF")
    assert model1.data.column_names == ["date", "Mkt-RF"]

    model2 = DummyModel()
    model2.extract(["Mkt-RF", "SMB"])
    assert set(model2.data.column_names) == {"date", "Mkt-RF", "SMB"}


def test_base_drop():
    """Test dropping factor columns and raising errors when dropping all."""
    model = DummyModel()
    model.drop("SMB")
    assert "SMB" not in model.data.column_names

    with pytest.raises(ValueError, match="Cannot drop all factors"):
        model.drop(["Mkt-RF", "SMB"])


@patch("getfactormodels.models.base.Base._evict_cache_keys", return_value=2)
def test_base_clear_cache(mock_evict):
    # Class method
    count_cls = DummyModel.clear_cache("ff3")
    assert count_cls == 2
    mock_evict.assert_called_with("ff3")

    # Instance method
    model = DummyModel()
    count_inst = model.clear_cache()
    assert count_inst == 2
    assert model._data is None


def test_base_dataframe_protocols():
    model = DummyModel()

    try:
        import pandas as pd
        df = model.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert df.index.name == "date"
    except ImportError:
        pass

    try:
        import polars as pl
        pl_df = model.to_polars()
        assert isinstance(pl_df, pl.DataFrame)
    except ImportError:
        pass

    protocol_df = model.__dataframe__()
    assert protocol_df is not None


class DummyPortfolio(PortfolioBase):
    @property
    def url(self) -> str:
        return "https://example.com/port.zip"

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([("date", pa.date32()), ("RET", pa.float64())])

    @property
    def frequencies(self) -> list[str]:
        return ["m"]

    def _read(self, data: bytes) -> pa.Table:
        return pa.Table.from_pydict({"date": ["2020-01-01"], "RET": [1.0]})


def test_portfolio_base_init():
    port = DummyPortfolio(sort="size", weights="ew", count=10, grid="2x3", dividends=False)
    assert port.sort == "size"
    assert port.weights == "ew"
    assert port.count == 10
    assert port.grid == "2x3"
    assert port.dividends is False

    with pytest.raises(ValueError, match="Weights must be either 'vw' or 'ew'"):
        DummyPortfolio(weights="invalid")


class DummyAnomaly(Anomalies):
    @property
    def url(self) -> str:
        return "https://example.com/anomaly.zip"

    @property
    def schema(self) -> pa.Schema:
        return pa.schema([("date", pa.date32()), ("mom", pa.float64())])

    @property
    def frequencies(self) -> list[str]:
        return ["m", "y", "w", "d", "q"]

    def _read(self, data: bytes) -> pa.Table:
        return pa.Table.from_pydict({"date": ["2020-01-01"], "mom": [1.0]})


def test_anomalies_init():
    anomaly_decile = DummyAnomaly(anomaly="mom", bivariate=False)
    assert anomaly_decile.anomalies == ["mom"]
    assert anomaly_decile.bivariate is False
    assert anomaly_decile.sort_type == "decile"

    anomaly_biv = DummyAnomaly(anomaly=["mom", "acc"], bivariate=True)
    assert anomaly_biv.anomalies == ["mom", "acc"]
    assert anomaly_biv.bivariate is True
    assert anomaly_biv.sort_type == "3x5"
