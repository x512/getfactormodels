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
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
import pandas as pd
import pyarrow as pa
from getfactormodels.utils.data_utils import (
    filter_table_by_date,
    rearrange_columns,
    round_to_precision,
    verify_cols_exist,
)
from getfactormodels.utils.http_client import _HttpClient
from getfactormodels.utils.utils import _save_to_file, _validate_date


class FactorModel(ABC):
    """Abstract Base Class used by all factor model implementations."""
    def __init__(self, frequency: str | None = 'm',
                 start_date: str | None  = None,
                 end_date: str | None = None,
                 output_file: str | None = None,
                 cache_ttl: int = 86400,
                 **kwargs: Any):
        """
        Initialize the factor model instance.

        Args
            frequency (str): the frequency of the data. Default: 'm'.
            start_date (str, opt):
            end_date (str, opt):
            output_file (str | Path, opt):
            cache_ttl (int): Cache time-to-live in seconds. Default: 86400.
            **kwargs: some models have additional params.
        """

        logger_name = f"{self.__module__}.{self.__class__.__name__}"
        self.log = logging.getLogger(logger_name)

        self._data: pa.Table | None = None
        self._df: pd.DataFrame | None = None  # new caching the df

        self._start_date = None
        self._end_date = None
        self._frequency = None

        self.frequency = frequency 
        self.start_date = start_date 
        self.end_date = end_date
        self.output_file = output_file
        self.cache_ttl = cache_ttl
        self.copyright: str = ""  # NEW, TEST. fix: Carhart erroring with FF with copyright

        self._selected_factors: list[str] | None = None  # for eg drop/extract
        super().__init__()
    
    def __repr__(self) -> str:
        # TODO: df repr, without pd, for pa. 
        params = [
            f"frequency='{self.frequency}'",
            f"start_date='{self.start_date}'",
            f"end_date='{self.end_date}'",
        ]

        if hasattr(self, 'region') and self.region:
            params.append(f"region='{self.region}'")

        if self.output_file:
            params.append(f"output_file='{self.output_file}'")

        return f"{self.__class__.__name__}({', '.join(params)})"

    def __len__(self) -> int:
        return len(self.data) # length of the df (after slicing)


    @property
    def start_date(self) -> str | None:
        return self._start_date
    @start_date.setter
    def start_date(self, value: Any):
        valid = _validate_date(value, is_end=False)
        if self._start_date != valid:
            self._start_date = valid
            self._df = None # make .data reslice the table

    @property
    def end_date(self) -> str | None:
        return self._end_date
    @end_date.setter
    def end_date(self, value: Any):
        valid = _validate_date(value, is_end=True)
        if self._end_date != valid:
            self._end_date = valid  # set 
            self._df = None

    @property
    def frequency(self) -> str | None: 
        return self._frequency
    @frequency.setter
    def frequency(self, value: str | None):
        if value is None:
            self._frequency = None
            return

        val = value.lower()
        if val not in self._frequencies:
            raise ValueError(f"Invalid '{val}'. Options: {self._frequencies}") 

        if val != self._frequency:
            if self._frequency is not None:
                self.log.info(f"Freq. changed from {self._frequency} to {val}.")
            self._frequency = val
            self._data = None
            self._df = None


    @property
    def data(self) -> pd.DataFrame:
        """Returns the sliced Pandas df."""
        if self._df is not None:
            return self._df

        table = self._get_table()
        sliced_table = filter_table_by_date(table, self.start_date, self.end_date)

        # pd containment zone ------------------------------ # 
        df = sliced_table.to_pandas(date_as_object=False)
        if 'date' in df.columns:
            # fix: carhart daily was returning 2014 only.
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date').sort_index()            
        self._df = df
        return self._df        
        # -------------------------------------------------- #


    def _extract_as_table(self, factor: str | list[str]) -> pa.Table:
        """Internal helper: Extracts factors (columns) and returns 
        a pa.Table."""
        table = self._get_table()
        factors = [factor] if isinstance(factor, str) else factor
        table = filter_table_by_date(table, self.start_date, self.end_date)
        # don't need to verify. Trust in the schema...
        return table.select(['date'] + factors)


    def extract(self, factor: str | list[str]) -> pd.DataFrame | pd.Series:
        """Select specific factors from the model.

        Args
            factor (str | list[str]): The column name(s) to extract. 
                Matches are case-sensitive.
        """
        factors = [factor] if isinstance(factor, str) else factor
        
        full_table = self._get_table()
        
        # Validate/prevent date index being extracted.
        validated = verify_cols_exist(full_table, factors)  
        if not validated or (len(validated) == 1 and validated[0] == 'date'):
             raise ValueError("Extraction must include at least one factor (cannot extract only 'date').")

        self._selected_factors = [f for f in validated if f != 'date']
        self._df = None
        
        return self.data[factor] if isinstance(factor, str) else self.data       


    def drop(self, factor: str | list[str]) -> pd.DataFrame:
        """Remove specific factors from the model.

        Args
            factor (str | list[str]): The column name(s) to remove.
                Matches are case-sensitive.
        """
        to_drop = [factor] if isinstance(factor, str) else factor
        full_table = self._get_table()
        
        valid_to_drop = verify_cols_exist(full_table, to_drop)
        
        # Calculate selection, error if all factors try to get dropped:
        new_selection = [
            c for c in full_table.column_names 
            if c not in valid_to_drop and c != 'date'
        ]
        if not new_selection:
            raise ValueError("Cannot drop all factors from the model.")
            
        self._selected_factors = new_selection
        self._df = None
        return self.data 


    def to_file(self, filepath: str | Path | None = None) -> None:
        """Save data to a file.
 
        Args:
            filepath (str | Path | None): the filepath to save data to. 
                Supports: .parquet, .ipc, .feather, .csv, .txt, .pkl, .md
        
        Example:
            .to_file() or .to_file('custom_name.pkl')
        ---
        Notes:
        - Used by the CLI via the --output flag.
        - If filepath is None, data is saved to user's CWD with generated filename.
        """
        target = filepath if filepath else self.output_file
        if not target:
            self.log.error("No filepath provided and no default output_file set.")
            return

        table = self._get_table()
        sliced_table = filter_table_by_date(table, self.start_date, self.end_date)

        if sliced_table.num_rows == 0:
            self.log.warning("No data available to save.")
            return

        _save_to_file(sliced_table, target, model_instance=self)


    def _get_table(self) -> pa.Table:
        """Internal: triggers download if cache empty. 
        Returns the table (filtered if extract/drop was called).
        """
        # Download/cache 
        if self._data is None:
            raw_bytes = self._download()
            table = self._read(raw_bytes)
            table = round_to_precision(table, self._precision)
            table.validate(full=True)
            self._data = rearrange_columns(table=table)
        
        # If not dropping or extracting:
        if not self._selected_factors:
            return self._data

        selection = list(dict.fromkeys(['date'] + self._selected_factors))
        return self._data.select(selection)


    # TODO: ff requires two files... allow list passed in
    def _download(self) -> bytes:
        url = self._get_url()
        log_msg = f"Downloading data from: {url}"
        self.log.info(log_msg)
        try:
            with _HttpClient(timeout=15.0) as client:
                return client.download(url, self.cache_ttl)
        except Exception as e:
            self.log.error(f"Failed to download from {url}: {e}")
            # crash fast on download failure
            raise RuntimeError(f"Download failed for {url}.") from e


    @property
    def _precision(self) -> int:
        return 8

    @property
    @abstractmethod
    def schema(self) -> pa.Schema:
        pass

    @property
    @abstractmethod
    def _frequencies(self) -> list[str]:
        pass

    @abstractmethod 
    def _get_url(self) -> str | list[str]:
        """Build the unique data source URL."""
        #TODO: change to accept a list (e.g., for FF mom models, backup URLs)
        pass

    @abstractmethod
    def _read(self, data: bytes) -> pa.Table:
        """Read bytes into a pa.Table."""
        pass
