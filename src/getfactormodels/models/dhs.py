# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
"""Daniel, Hirshleifer and Sun (2020) Behavioural Factors."""

import io
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.arrow_utils import (
    scale_to_decimal,
    select_table_columns,
)
from getfactormodels.utils.date_utils import offset_period_eom


class DHSFactors(FactorModel):
    """The Behavioural Factors of Daniel, Hirshleifer and Sun (2020).
    
    FIN and PEAD factors. Data from July 1972 - December, 2023. Adds 
    market excess and risk-free rate from Fama/French.

    References:
        K. Daniel, D. Hirshleifer and L. Sun, 2020. Short and Long Horizon
        Behavioral Factors. Review of Financial Studies, 33 (4): 1673-1736.
    """
    @property
    def frequencies(self) -> list[str]: return ['d', 'm']
    
    @property
    def _precision(self) -> int: return 12

    @property
    def url(self) -> str:
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            sheet_id = '15wTRI_vf7pXSyEW1VeYkNIGWxRyUtBLf'
            #info_id =
        else:
            sheet_id = '1K78rVuWEW6doD7ZdMVQCRJ4z0azM50vP'
            #info_sheet_id = '#gid=96292754'  # Construction, Universe, Period, More Details

        return  f'{base_url}{sheet_id}/export?format=xlsx'

    @property  
    def schema(self) -> pa.Schema:
        """DHS factor model schema."""
        date = [('Date', pa.string())]   # 197202
        fin = [('FIN', pa.float64())]
        pead = [('PEAD', pa.float64())]
        
        # FIN and PEAD, swap cols between frequencies.
        if self.frequency == 'd':
            return pa.schema(date + fin + pead)
        return pa.schema(date + pead + fin)

    # Precision fix: export as .xlsx, then read with Calamine. 
    def _read(self, data: bytes) -> pa.Table:
        wb = CalamineWorkbook.from_filelike(io.BytesIO(data))

        rows = wb.get_sheet_by_name(wb.sheet_names[0]).to_python()
        headers = [str(h).strip() for h in rows[0]]

        _dict = {name: [row[i] for row in rows[1:]] for i, name in enumerate(headers)}

        # fix: load without schema here
        table = pa.Table.from_pydict(_dict)

        # Enforce schema here
        table = table.select(self.schema.names).cast(self.schema)
        
        table = scale_to_decimal(table)
        table = offset_period_eom(table, self.frequency)
        
        if "Date" in table.column_names:
            table = table.rename_columns(["date"] + table.column_names[1:])

        # fix: freq handed to ff might be None, default to m. appease pyright.
        ff_frequency = self.frequency or "m"
        ff_model = FamaFrenchFactors(model="3", frequency=ff_frequency)
        ff_model.load()
        
        if ff_model.data is None:
            raise ValueError("Failed to load FamaFrenchFactors for DHS model.")

        ff_table = select_table_columns(ff_model.data, ['Mkt-RF', 'RF'])

        return table.join(ff_table, keys="date", join_type="inner").combine_chunks()
