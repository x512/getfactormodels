<a name="readme-top"></a>

# getfactormodels

[![Python](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fx512%2Fgetfactormodels%2Frefs%2Fheads%2Fmain%2Fpyproject.toml&query=project.requires-python&label=python&logo=python&logoColor=ffde57&style=flat-square)]([https://python.org](https://www.python.org/downloads/))
![PyPI - Version](https://img.shields.io/pypi/v/getfactormodels?style=flat-square&label=PyPI)
![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square&labelColor=%23313131)
![GitHub License](https://img.shields.io/github/license/x512/getfactormodels?style=flat-square&logoSize=auto&labelColor=%23313131&color=%234EAA25&cacheSeconds=3600&link=https%3A%2F%2Fgithub.com%2Fx512%2Fgetfactormodels%2Ftree%2Fmain%3Ftab%3Dreadme-ov-file%23license)

A CLI and Python library to download multi-factor asset pricing models.

## Quick start
### Install


## Models

- The 3-factor, 5-factor, and 6-factor models of Fama & French <sup>[[1]](#1) [[3]](#3) [[4]](#4)</sup>
- Mark Carhart's 4-factor model <sup>[[2]](#2)</sup>
- Pastor and Stambaugh's liquidity factors <sup>[[5]](#5)</sup>
- Mispricing factors of Stambaugh and Yuan<sup>[[6]](#6)</sup>
- The $q$*-factor* model of Hou, Mo, Xue and Zhang<sup>[[7]](#7)</sup>
- The augmented $q^5$*-factor* model of  Hou, Mo, Xue and Zhang<sup>[[8]](#8)</sup>
- *Intermediary Capital Ratio* (ICR) of He, Kelly & Manela<sup>[[9]](#9)</sup>
- The *DHS behavioural factors* of Daniel, Hirshleifer & Sun<sup>[[10]](#10)</sup>
- The *HML* $^{DEVIL}$ factor of Asness & Frazzini<sup>[[11]](#11)</sup>
- *Betting Against beta*, A. Frazzini, L. Pedersen (2014) <sup>[[12]](#12)</sup>
- *Quality Minus Junk*, Asness, Frazzini & Pedersen (2017)<sup>[[13]](#13)</sup>
- The 6-factor model of Barillas and Shanken<sup>[[14]](#14)</sup>


_Thanks to: Kenneth French, Robert Stambaugh, Lin Sun, Zhiguo He, AQR Capital Management (AQR.com) and Hou, Xue and Zhang (global-q.org), for their research and for the datasets they provide._



## Quick start

![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square)
>[!IMPORTANT]
>``getfactormodels`` is pre-alpha (until version 0.1.0), don't rely on it for anything.
>
>*But a huge thanks to anyone who has tried it!*

### Install

Install `getfactormodels` via PyPI.

```bash
pip install getfactormodels
```
---
### Usage

Examples of CLI usage:

Specify a model, and an optional frequency (default: `m`), and date range:

    getfactormodels --model ff3 --frequency m --start 2010-01-01 --end 2020
- see: `--list-models`


Download the Fama-French 5 Factor model for emerging markets, save to file:

    getfactormodels -m ff5 -f m -s 2010-01 -e 2020 --region emerging --output 'ff5_emerging.csv'
    
- Use `--list-regions` to see all available geographic regions and aliases.


Download a Global-Q anomaly testing portfolio (weekly, deciles for roe_6, ta)

    getfactormodels --anomaly roe_6 ta --sort decile -f w -o '~/file.md'

- see: `--list-anomalies`
- `-o` / `--output` automatically detects format from extensions (`.csv`, `.parquet`, `.feather`).


Access JKP Global Factor data for a region:

    getfactormodels -j age -r can --drop-nulls


Attach a JKP Global Factor to a Betting Against Beta model for Australia (since both have regional data available): 

    getfactormodels -m bab -r aus -j age


Combine the Liquidity, DHS Behavioural Factor models (of the same frequency), drop the NaNs and view the source data URLs: 

    getfactormodels -m liq dhs -f m -e 1999-12-31 --drop-nulls --url


Download the augmnented q5 model, and quintiles for a Global Q Anomaly testing portfolio: 
    
    getfactormodels -m q -f m -p abr_1 -n 5 -o `model.ipc`


Download a 2x3 size/OP sort (`--sort`/`-n`) from Kenneth French's data library:

    getfactormodels -p size op --sort 2x3


Download a 5x5 sort on Inv and Book-to-Market using the --bivariate flag (defaults to 5x5 for inv/op) 

    getfactormodels -p 'inv, bm' -b


Download Fama-French industry data:

    getfactormodels -p industry 38 --drop-nulls 'GOVT'
    #or 
    getfactormodels -I 38 --drop-null 'GOVT'


Drop columns: 

    getfactormodels -m ff3 -d SMB,RF 


Download Fama-French Breakpoint data:

    # E/P
    getfactormodels --bp ep 

    #prior rets
    getfactormodels --breakpoints prior 




### Python Examples
```python
from getfactormodels import model, portfolio

# Load Fama-French 3-factor monthly model
ff3 = model("ff3", frequency="m", start_date="2010-01-01")
ff3.load()


# Export to CSV
a.to_file("abr_1_data.parquet")
```

#### Model classes
```py
from getfactormodels import FamaFrenchFactors

# Initialize model instance
m = FamaFrenchFactors(
    model="3", frequency="m", region="developed", start_date="2020-01-01"
)
m.end_date = "2020"

# Download the data
m = m.load()

# Access/download the Arrow Table:
table = m.data

# As a dataframe:
df = m.to_polars()  # Helper method, see also `.to_pandas()`
```

```python
from getfactormodels import Qfactors, BABFactors

# Q Factors have a "classic" boolean, when true, returns the classic 4 factor model.
q = QFactors(classic=True, frequency='w').load()

# Extract the Japan Betting Against Beta daily 'BAB' factor:
bab_jpn_df = BABFactors(frequency='d', region='JPN', 
                        start='2000-02-20', end '2010').load().extract("BAB").to_polars()
```
---

**Example:**

    getfactormodels -m qmj -f d --output qmj.ipc

<details>
<summary>View output</summary>

```plaintext
Data saved to: qmj.ipc

date            Mkt-RF           QMJ           SMB           HML           UMD    RF_AQR
1957-07-01    0.001784     -0.001566     -0.002166      0.001984      0.000651    0.0001
1957-07-02    0.008514     -0.000484     -0.005030     -0.004436      0.002705    0.0001
1957-07-03    0.007938      0.000869     -0.001245     -0.003676      0.002294    0.0001
1957-07-05    0.007755      0.001975     -0.000769     -0.002781     -0.001137    0.0001
  [...]
2025-10-28    0.001157      0.004138     -0.003897     -0.006681      0.011682    0.0002
2025-10-29   -0.002034     -0.003446     -0.007738     -0.002619      0.015875    0.0002
2025-10-30   -0.010841      0.009058     -0.000406      0.005180     -0.006211    0.0002
2025-10-31    0.003867     -0.006546      0.000620      0.001108      0.000869    0.0002

[17574 rows x 7 columns, 905.3 kb]
```
</details>

### Python usage
- `getfactormodels` uses PyArrow internally and supports the Dataframe Interchange Protocol, allowing for zero-copy data sharing with most modern Python data tools.

Create a model instance:
```py
from getfactormodels import QFactors

m = getfactormodels.QFactors(frequency="m")
```

- DuckDB can query a table without conversion
```py
import duckdb

duckdb.sql("SELECT date, ROE, IA FROM m.data LIMIT 7").show()
```

- Polars has first-class support for Arrow:
```py
import polars as pl

df = pl.from_arrow(m.data)
```

- Pandas/NumPy
```py
# Pandas DataFrame
df = m.to_pandas()

# or NumPy Array (via Pandas)
array = m.to_pandas().to_numpy()
```

- Libraries like Ibis, Modin or Vaex can use the dataframe interchange protocol directly:

```py
df = vaex.from_arrow_table(m.data)
print(df.mean(vdf.ROE))
```
<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Availability

_This table shows each model's start date, available frequencies, and the latest datapoint if not current. The ``id`` column 
contains the shortest identifier for each model. These should all work in python and the CLI._

| `id` | Factor Model| Start  | D       | W     | M     | Q     | Y      | End  |
|:--:|:--------------|:----------:|--------------|--------------|--------------|--------------|--------------|:----------:|
|`3`| Fama-French 3 | 1926-07-01 | ✓            | ✓   | ✓         |      | ✓ |     -  |
|`4`| Carhart 4      | 1926-11-03 | ✓ |              | ✓ |     | ✓ |     -    |
|`5`| Fama-French 5  | 1963-07-01 | ✓ |              | ✓ |    | ✓ |     -     |
|`6`| Fama-French 6 | 1963-07-01 | ✓ |              | ✓ |       | ✓ |      -  |
|`icr`| ICR           | 1970-01-31<br><sub>*Daily: 1999-05-03</sub>* | ✓ ||✓| ✓ |    | 2025-06-27 |
|`dhs`| DHS          | 1972-07-03 | ✓ |            | ✓ |     |                       | 2024-12-31 |
|`mis`| Mispricing    | 1963-01-02 | ✓ |            | ✓ |              |             | 2016-12-30 |
|`liq`| Liquidity     | 1962-08-31 |   |      | ✓ |      |                           | 2025-12-31 |
|`q`<br>`q4`| $q^5$-factors<br>$q$-factors | 1967-01-03 | ✓ | ✓ |✓ | $\checkmark$ | ✓| 2024-12-31 |
|`hmld`| HML $^{DEVIL}$ | 1926-07-01       | ✓ |         | ✓ |       |               | 2025-10-31 |
|`qmj`| Quality Minus Junk | 1957-07-01    | ✓ |         | ✓ |       |               | 2025-10-31 |
|`bab`| Betting Against beta | 1930-12-01  | ✓ |         | ✓ |       |               | 2025-10-31 |
|`bs`| Barillas-Shanken 6 | 1967-01-03       | ✓ |           |✓ |      |             | 2024-12-31 |
|`ch3`| Size and Value in China (CH-3) | 2000-01-04       | ✓ |           |✓ |      |             | 2026-08-31 |
|`ch4`| Size and Value in China (CH-4) | 2000-01-04       | ✓ |           |✓ |      |             | 2026-08-31 |

* Fama-French: data up until until end of prior month.
* Fama-French: most international/emerging factors (accessed with the region param) begin between 1985-1990.
* AQR models: non-US data begins around 1990 (accessed with the country param).
* Size and Value in China models are *only* available for China. `region=` must be `ch`, `china` or `None` (`--region ch`)
* ``getfactormodels`` has two conditional CAPM (CCAPM) models: a High-Income/Affluent CCAPM (annual, 1927-2022) from Campbell & Korniotis (2013)<sup>[[15]](#15)</sup>, and an implementation of the Premium-Labor CCAPM (m, q, y; data from 1959-04-30) from Jagganathan & Wang (1996)<sup>[[16]](#16)</sup>


## References

**Publications:**

1. <a id="1"></a> E. F. Fama and K. R. French, ‘Common risk factors in the returns on stocks and bonds’, *Journal of Financial Economics*, vol. 33, no. 1, pp. 3–56, 1993. [PDF](https://people.duke.edu/~charvey/Teaching/BA453_2006/FF_Common_risk.pdf)
2. <a id="2"></a> M. Carhart, ‘On Persistence in Mutual Fund Performance’, *Journal of Finance*, vol. 52, no. 1, pp. 57–82, 1997. [PDF](https://onlinelibrary.wiley.com/doi/full/10.1111/j.1540-6261.1997.tb03808.x)
3. <a id="3"></a> E. F. Fama and K. R. French, ‘A five-factor asset pricing model’, *Journal of Financial Economics*, vol. 116, no. 1, pp. 1–22, 2015. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2287202)
4. <a id="4"></a> E. F. Fama and K. R. French, ‘Choosing factors’, *Journal of Financial Economics*, vol. 128, no. 2, pp. 234–252, 2018. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2668236)
5. <a id="5"></a>L. Pastor and R. Stambaugh, ‘Liquidity Risk and Expected Stock Returns’, *Journal of Political Economy*, vol. 111, no. 3, pp. 642–685, 2003. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=279804)
6. <a id="6"></a>R. F. Stambaugh and Y. Yuan, ‘Mispricing Factors’, *The Review of Financial Studies*, vol. 30, no. 4, pp. 1270–1315, 12 2016. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2626701)
7. <a id="7"></a>K. Hou, H. Mo, C. Xue, and L. Zhang, ‘Which Factors?’, *National Bureau of Economic Research, Inc*, 2014. [PDF](https://academic.oup.com/rof/article/23/1/1/5133564)
8. <a id="8"></a>K. Hou, H. Mo, C. Xue, and L. Zhang, ‘An Augmented q-Factor Model with Expected Growth*’, *Review of Finance*, vol. 25, no. 1, pp. 1–41, 02 2020. [PDF](https://academic.oup.com/rof/article/25/1/1/5727769)
9. <a id="9"></a>Z. He, B. Kelly, and A. Manela, ‘Intermediary asset pricing: New evidence from many asset classes’, *Journal of Financial Economics*, vol. 126, no. 1, pp. 1–35, 2017. [PDF](https://cpb-us-w2.wpmucdn.com/voices.uchicago.edu/dist/6/2325/files/2019/12/jfepublishedversion.pdf)
10. <a id="10"></a>K. Daniel, D. Hirshleifer, and L. Sun, ‘Short- and Long-Horizon Behavioral Factors’, *Review of Financial Studies*, vol. 33, no. 4, pp. 1673–1736, 2020. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3086063)
11. <a id="11"></a>C. Asness and A. Frazzini, ‘The Devil in HML’s Details’, *The Journal of Portfolio Management*, vol. 39, pp. 49–68, 2013. [PDF](https://stockmarketmba.com/docs/Asness_Frazzini_AdjustHML.pdf)
12. <a id="12"></a>A. Frazzini and L. H. Pedersen, “Betting Against Beta,” Journal of Financial Economics, vol. 111, no. 1, pp. 1–25, Jan. 2014. [EconPapers](https://econpapers.repec.org/paper/nbrnberwo/16601.htm)[PDF (working paper)](https://www.nber.org/system/files/working_papers/w16601/w16601.pdf) 
13. <a id="13"></a>C. S. Asness, A. Frazzini, and L. H. Pedersen, “Quality Minus Junk,” Review of Accounting Studies, vol. 24, no. 1, pp. 34–112, Nov. 2019. [EconPapers](https://econpapers.repec.org/article/sprreaccs/v_3a24_3ay_3a2019_3ai_3a1_3ad_3a10.1007_5fs11142-018-9470-2.htm) [PDF](https://link.springer.com/content/pdf/10.1007/s11142-018-9470-2.pdf)
14. <a id="14"></a>F. Barillas and J. Shanken, ‘Comparing Asset Pricing Models’, *Journal of Finance*, vol. 73, no. 2, pp. 715–754, 2018. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2700000)
15. <a id="15"></a>S. D. Campbell and G. M. Korniotis, "The Human Capital That Matters: Expected Returns and the Income of Affluent Households," Finance and Economics Discussion Series, BiblioGov, 2013. [Web](https://www.federalreserve.gov/pubs/feds/2008/200809/index.html)
16. <a id="16"></a>R. Jagannathan and Z. Wang, ‘The Conditional CAPM and the Cross-Section of Expected Returns’, The Journal of Finance, vol. 51, no. 1, pp. 3–53, Mar. 1996.

**Data sources:**

* K. French, "Data Library," Tuck School of Business at Dartmouth.
  [Link](https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html)
* R. Stambaugh, "Liquidity" and "Mispricing" factor datasets, Wharton School, University of Pennsylvania.
[Link](https://finance.wharton.upenn.edu/~stambaug/)
* Z. He, "Intermediary Capital Ratio and Risk Factor" dataset, zhiguohe.net. [Link](https://zhiguohe.net/data-and-empirical-patterns/intermediary-capital-ratio-and-risk-factor/)
* K. Hou, G. Xue, R. Zhang, "The Hou-Xue-Zhang q-factors data library," at global-q.org.
[Link](http://global-q.org/factors.html)
* AQR Capital Management's Data Sets.
* Lin Sun, DHS Behavioural factors [Link](https://sites.google.com/view/linsunhome)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License
![GitHub License](https://img.shields.io/github/license/x512/getfactormodels?style=flat-square&logoSize=auto&labelColor=%23313131&color=%234EAA25&cacheSeconds=3600&link=https%3A%2F%2Fgithub.com%2Fx512%2Fgetfactormodels%2Ftree%2Fmain%3Ftab%3Dreadme-ov-file%23license)

### Known issues
* AQR Models (HML Devil, Betting Against Beta, Quality Minus Junk) download slowly, particulary daily datasets. Need to implement a progress bar.

##### Todo
- Documentation
- Example notebook
- Error handling
- README
- Refactor of FF models
- metadata - copyright, license, bibtex/citations
