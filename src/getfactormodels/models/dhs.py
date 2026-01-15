# getfactormodels: https://github.com/x512/getfactormodels
# Copyright (C) 2025-2026 S. Martin <x512@pm.me>
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Distributed WITHOUT ANY WARRANTY. See LICENSE for full terms.
import io
from typing import Any
import pyarrow as pa
from python_calamine import CalamineWorkbook
from getfactormodels.models.base import FactorModel
from getfactormodels.models.fama_french import FamaFrenchFactors
from getfactormodels.utils.arrow_utils import (
    round_to_precision,
    scale_to_decimal,
    select_table_columns,
)
from getfactormodels.utils.date_utils import offset_period_eom


class DHSFactors(FactorModel):
    """The Behavioural Factors of Daniel, Hirshleifer and Sun (2020).
    
    FIN and PEAD factors. Data from July 1972 - December, 2023.

    References:
        K. Daniel, D. Hirshleifer and L. Sun, 2020. Short and Long Horizon
        Behavioral Factors. Review of Financial Studies, 33 (4): 1673-1736.

    """
    # copyright/attribution info! TODO
    @property
    def _frequencies(self) -> list[str]: return ['d', 'm']
    
    @property
    def _precision(self) -> int: return 12

    @property  
    def schema(self) -> pa.Schema:
        date = [('Date', pa.string())]   # 197202
        fin = [('FIN', pa.float64())]
        pead = [('PEAD', pa.float64())]
        
        # FIN and PEAD, swap cols between frequencies.
        if self.frequency == 'd':
            return pa.schema(date + fin + pead)
        return pa.schema(date + pead + fin)


    def _get_url(self) -> str:
        base_url = 'https://docs.google.com/spreadsheets/d/'

        if self.frequency == 'd':
            sheet_id = '1lWaNCuHeOE-nYlB7GA1Z2-QQa3Gt8UJC'
            #info_id =
        else:
            sheet_id = '1VwQcowFb5c0x3-0sQVf1RfIcUpetHK46'
            #info_sheet_id = '#gid=96292754'  # Construction, Universe, Period, More Details

        return  f'{base_url}{sheet_id}/export?format=xlsx'


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

        # Wrap in FF Mkt-RF and RF
        _ff = FamaFrenchFactors(model='3', 
                                frequency=self.frequency).load()
        _ff = select_table_columns(_ff.data, ['Mkt-RF', 'RF'])

        return table.join(_ff, keys="date", join_type="inner").combine_chunks()
