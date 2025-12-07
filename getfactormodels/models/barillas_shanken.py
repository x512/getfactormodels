# Placeholder
from . import FamaFrenchFactors, QFactors, HMLDevil
import pandas as pd

class BarillasShankenFactors:
    """Class for Barillas-Shanken 6-Factor Model"""
    def __init__(self, frequency='m', start_date=None, end_date=None,
                 output_file=None):
        self.frequency = frequency
        self.start_date = start_date
        self.end_date = end_date
        self.output_file = output_file

    def download(self) -> pd.DataFrame:
        """Download the Barillas-Shanken 6 Factor Model (2018).

        A combination of the 5-factor model of Fama and French (2015), the q-factor
        model of Hou, Xue, and Zhang (2015), and Asness and Frazzini's HML Devil.
        This is the factor model with the highest posterior inclusion probability
        in Barillas and Shanken (2018).

        Note:
            - Relies on the HML Devil factors being retrieved (which is very slow).

        Returns:
            pd.DataFrame: A timeseries of the factor data.
        """
        return self._download()

    def _download(self) -> pd.DataFrame:
        """Constructs the Barillas 6 factor model from other models"""

        print("  - Getting q factors...")
        qdata = QFactors(frequency=self.frequency, classic=True)
        q = qdata.download()
        q = q[['R_IA', 'R_ROE']]

        print("  - Getting Fama-French factors...")
        ffdata = FamaFrenchFactors(model='6', frequency=self.frequency)
        ff = ffdata.download()

        ff = ff[['Mkt-RF', 'SMB', 'UMD']]

        # Merge q and Fama-French factors
        df = q.merge(ff, left_index=True, right_index=True, how='inner')

        print("  - Getting HML_Devil factor (this can take a while, please be patient)...")
        hmld_data = HMLDevil(frequency=self.frequency,
                                 start_date=self.start_date,
                                 end_date=self.end_date)

        hml_d = hmld_data.download()

        # NOTE: Taking the 'RF' from AQR's series since it's here, and it's the
        #  same data as Fama-French but to 4 decimals. Mkt-RF shows a difference
        #  though -- and bs6 should use the mkt-rf of ff!
        hml_d = hml_d[['HML_Devil', 'RF']]

        hml_d.index.name = 'date'

        df = df.merge(hml_d, left_index=True, right_index=True, how='inner')

        # filter by date
        if self.start_date:
            start_dt = pd.to_datetime(self.start_date)
            df = df[df.index >= start_dt]
        if self.end_date:
            end_dt = pd.to_datetime(self.end_date)
            df = df[df.index <= end_dt]

        return df
            # TODO: CACHE! FILE WRITER! FIXME TODO
            #if self.output_file:
            #    df.to_csv(self.output_file)
            #    print(f"Data saved to {self.output_file}")


