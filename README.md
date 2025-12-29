<a name="readme-top"></a>

# getfactormodels

[![Python](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fx512%2Fgetfactormodels%2Frefs%2Fheads%2Fmain%2Fpyproject.toml&query=project.requires-python&label=python&logo=python&logoColor=ffde57&style=flat-square)]([https://python.org](https://www.python.org/downloads/))
![PyPI - Version](https://img.shields.io/pypi/v/getfactormodels?style=flat-square&label=PyPI)
![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square&labelColor=%23313131)
![GitHub License](https://img.shields.io/github/license/x512/getfactormodels?style=flat-square&logoSize=auto&labelColor=%23313131&color=%234EAA25&cacheSeconds=3600&link=https%3A%2F%2Fgithub.com%2Fx512%2Fgetfactormodels%2Ftree%2Fmain%3Ftab%3Dreadme-ov-file%23license)

A command-line tool to retrieve data for multi-factor asset pricing models.


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
- The 6-factor model of Barillas and Shanken<sup>[[12]](#12)</sup>


_Thanks to: Kenneth French, Robert Stambaugh, Lin Sun, Zhiguo He, AQR Capital Management (AQR.com) and Hou, Xue and Zhang (global-q.org), for their research and for the datasets they provide._


## Installation

>[!IMPORTANT]
>``getfactormodels`` is pre-alpha (until version 0.1.0), don't rely on it for anything.
>
>![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square)
>
>*But a huge thanks to anyone who has tried it!*

**Requires:**

- Python >=3.10


The easiest way to install `getfactormodels` is with pip:


```bash
pip install getfactormodels
```

## Quick start

### CLI

```bash 
# Fama-French 3-Factor (monthly) starting Jan 2020
getfactormodels -m ff3 -f m -s 2020-01-01 -o my_factors.csv

# ICR factors (daily) data saved to Parquet
getfactormodels -m icr -f d -s 2015-01-01 -o data.parquet

# Fama-French 6-Factor (Europe) with factor dropping
getfactormodels -m ff6 -f m --region europe --drop "RF" -o europe_factors.csv

```
### Python
**`get_factors`**

```py 
from getfactormodels import DHSFactors

model = DHSFactors(
    frequency='m',
    start_date='2000-01-01',
    end_date='2024-12-31',
    output_file='data.csv',
    cache_ttl=86400
)
df = model.data
model.to_file("factors.md")
```

**Model classes**

```py
from getfactormodels import FamaFrenchFactors, DHSFactors, BarillasShankenFactors, QFactors

# Fama-French 3-Factor
ff3 = FamaFrenchFactors(model='3', frequency='m', region='developed', start_date='2020-01-01')

ff3.end_date = '2020'
ff3.frequency = 'd'

df_ff3 = ff3.data

# Q Factors have a "classic" boolean, when true, returns the classic 4 factor model.
q = QFactors(classic=True, frequency='w').data

misp = gfm.MispricingFactors(frequency='m')
df = misp.data
```

*A list of model classes available:*
 - `FamaFrenchFactors`
 - `CarhartFactors`
 - `QFactors`
 - `ICRFactors`
 - `DHSFactors`
 - `LiquidityFactors`
 - `MispricingFactors`
 - `HMLDevilFactors`
 - `BarillasShankenFactors`


<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Data Availability

_This table shows each model's start date, available frequencies, and the latest datapoint if not current. The ``id`` column 
contains the shortest identifier for each model. These should all work in python and the CLI._

| `id` | Factor Model| Start  | D       | W     | M     | Q     | Y      | End  |
|:--:|:--------------|:----------:|--------------|--------------|--------------|--------------|--------------|:----------:|
|`3`| Fama-French 3 | 1926-07-01 | ✓            | ✓   | ✓         |      | ✓ |     -  |
|`4`| Carhart 4      | 1926-11-03 | ✓ |              | ✓ |     | ✓ |     -    |
|`5`| Fama-French 5  | 1963-07-01 | ✓ |              | ✓ |    | ✓ |     -     |
|`6`| Fama-French 6 | 1963-07-01 | ✓ |              | ✓ |       | ✓ |      -  |
|`hmld`| HML $^{DEVIL}$ | 1990-07-02  | ✓ |         | ✓ |        |       |-|
|`dhs`| DHS          | 1972-07-03 | ✓ |            | ✓ |     |      | 2023-12-29 |
|`icr`| ICR           | 1970-01-31<br><sub>*Daily: 1999-05-03</sub>* | ✓ ||✓| ✓ | | 2025-06-27 |
|`mis`| Mispricing    | 1963-01-02 | ✓ |            | ✓ |              |              | 2016-12-30 |
|`liq`| Liquidity     | 1962-08-31 |   |      | ✓ |      |    | 2024-12-31 |
|`q`<br>`q4`| $q^5$-factors<br>$q$-factors | 1967-01-03 | ✓ | ✓ |✓ | $\checkmark$ | ✓| 2022-12-30|
|`bs`| Barillas-Shanken | 1967-01-03 | ✓ |           |✓ |      |       | 2024-12-31 |


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
12. <a id="12"></a>F. Barillas and J. Shanken, ‘Comparing Asset Pricing Models’, *Journal of Finance*, vol. 73, no. 2, pp. 715–754, 2018. [PDF](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2700000)

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
* HML Devil: initial download is slow, particulary for daily data.

##### Todo
- Documentation
- Example notebook
- better error handling
- HML Devil: progress bar on download, smarter caching.
- this README
- metadata on model (copyright, construction, factors)
- Drop pandas
- Refactor of FF models

