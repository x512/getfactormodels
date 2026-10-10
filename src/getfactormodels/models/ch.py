# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
import httpx
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel, RegionMixin
from getfactormodels.utils.date_utils import offset_period_eom 


class CHFactors(FactorModel, RegionMixin):
    """Size and Value in China of Liu, Stambaugh & Yuan (2019).

    The CH-3 (SMB, VMG) and CH-4 (SMB, VMG, PMO) models. China data only. 

    Note: This model returns the Mkt-RF and RF for China.
    
    - Liu, J., Stambaugh, R. F., & Yuan, Y. (2019). Size and value in 
      China. Journal of Financial Economics, 134(1), 48–69.
    """
    @property
    def _frequencies(self) -> list[str]: return ['d', 'm']
    
    @property
    def _precision(self) -> int: return 4

    @property 
    def _regions(self) -> list[str]:
        return ["china", "ch"]

    def __init__(self, 
                 frequency: str = 'm', 
                 model: int | str = '3',
                 region: str | None = None, # China only 
                 **kwargs) -> None:

        # Validations
        freq = str(frequency).lower().strip()
        if freq not in self._frequencies:
            raise ValueError(f"Invalid frequency ('{frequency}'), must be one of {self._frequencies}.")

        model_str = str(model).lower().replace('ch', '').strip()
        if model_str not in ["3", "4"]:
            raise ValueError(f"Invalid model ('{model}'), must be 3 or 4.")

        if region is not None:
            reg_lower = str(region).lower().strip()
            if reg_lower not in self._regions:
                raise ValueError(f"Invalid region ('{region}'). Size and Value in China models only accept: {self._regions}.")
            _region = reg_lower
        else:
            _region = 'ch'

        kwargs.pop('model', None)
        
        super().__init__(frequency=freq, model=model_str, region=_region, **kwargs)
        self.model = model_str
        self.region = _region

    @property
    def schema(self) -> pa.Schema:
        if self.frequency == "d":
            date_col = [('date', pa.string())] 
            rf_col = [('rf_dly', pa.float64())]
        else:
            date_col = [('mnthdt', pa.string())] 
            rf_col = [('rf_mon', pa.float64())]
            
        mktrf_col = [('mktrf', pa.float64())]
        
        factors = [
            ("SMB", pa.float64()),
            ("VMG", pa.float64()),
        ]
        if self.model == "4":
            factors.append(("PMO", pa.float64()))

        return pa.schema(date_col + mktrf_col + rf_col + factors)


    @property
    def model(self) -> str:
        return self._model

    @model.setter
    def model(self, value: int | str):
        val = str(value).lower().replace('ch', '').strip()
        if val not in ["3", "4"]:
            raise ValueError(f"Invalid model ('{value}'), must be 3 or 4.")
        
        if hasattr(self, '_model') and val != self._model:
            self.log.info(f"Model changed from {self._model} to {val}")
            self._data = None
            self._view = None
        self._model = val


    def _get_url(self) -> str:
        base_url = 'https://en.mingshiim.com/static/'
        _freq = 'monthly' if self.frequency == 'm' else 'daily'
        return f"{base_url}CH{self.model}_factors_{_freq}_202608.xlsx" # URLs gonna change, FIXME TODO


    # TEMP FIX: FIXME: TODO: bypass SSL with verify=False works, verify=ssl_context wasn't.
    def _download(self, client=None) -> bytes:
        """Download using client with SSL bypass."""
        url = self._get_url()
        self.log.info(f"Downloading CHFactors from {url}")
        
        with httpx.Client(verify=False, follow_redirects=True) as http_client:
            resp = http_client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            return resp.content


    def _read(self, data: bytes) -> pa.Table:
        workbook = CalamineWorkbook.from_filelike(io.BytesIO(data))
        rows = workbook.get_sheet_by_index(0).to_python()
        if not rows:
            raise ValueError("Excel sheet is empty.")

        headers = [str(h).strip() for h in rows[0] if h is not None]
        data_rows = rows[1:]

        table_data = {
            name: [None if row[i] in ("", "None", None) else row[i] for row in data_rows]
            for i, name in enumerate(headers)
        }

        table = pa.Table.from_pydict(table_data)
        table = table.select(self.schema.names).cast(self.schema)
        table = offset_period_eom(table, self.frequency)

        rename_map = {'mktrf': 'MKTRF_CH',
                      'mnthdt': 'date',
                      'rf_mon': 'RF_CH',
                      'rf_dly': 'RF_CH',
                      'rf': 'RF_CH'}
        final_names = [rename_map.get(name.lower(), name) for name in table.column_names]
        table = table.rename_columns(final_names)

        if 'RF_CH' in table.column_names:
            cols = [c for c in table.column_names if c != 'RF_CH'] + ['RF_CH']
            table = table.select(cols)

        return table.combine_chunks()
