<a name="readme-top"></a>

# getfactormodels
[![Python](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fx512%2Fgetfactormodels%2Frefs%2Fheads%2Fmain%2Fpyproject.toml&query=project.requires-python&label=python&logo=python&logoColor=ffde57&style=flat-square)]([https://python.org](https://www.python.org/downloads/))
![PyPI - Version](https://img.shields.io/pypi/v/getfactormodels?style=flat-square&label=PyPI)
![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square&labelColor=%23313131)
![GitHub License](https://img.shields.io/github/license/x512/getfactormodels?style=flat-square&logoSize=auto&labelColor=%23313131&color=%234EAA25&cacheSeconds=3600&link=https%3A%2F%2Fgithub.com%2Fx512%2Fgetfactormodels%2Ftree%2Fmain%3Ftab%3Dreadme-ov-file%23license)

Reliably retrieve data for various multi-factor asset pricing models.

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

**Requirements:**

- Python ``>=3.10``.

The easiest way to install getfactormodels is via ``pip``:

  ```
  $ pip install getfactormodels -U
  ```

## Usage 
>[!IMPORTANT]
>![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square)
>
>``getfactormodels`` is pre-alpha (until version 0.1.0), don't rely on it for anything.


*Really quick usage notes until I rewrite it...*

### Python 

- In Python, import the getfactormodels package and call the get_factors()
function:

    ```py
    import getfactormodels
    
    data = getfactormodels.get_factors(model='carhart', frequency='d')
    ```
    ```
    >>> print(data)

                Mkt-RF     SMB     HML      RF     MOM
    date
    1926-11-03  0.0020 -0.0020 -0.0033  0.0001  0.0054
    1926-11-04  0.0059 -0.0012  0.0065  0.0001 -0.0051
    1926-11-05  0.0007 -0.0011  0.0026  0.0001  0.0117
    1926-11-06  0.0015 -0.0029  0.0005  0.0001 -0.0003
    1926-11-08  0.0052 -0.0012  0.0018  0.0001 -0.0002
    ...            ...     ...     ...     ...     ...
    2025-10-27  0.0117 -0.0056 -0.0121  0.0002  0.0047
    2025-10-28  0.0018 -0.0034 -0.0061  0.0002  0.0080
    2025-10-29 -0.0009 -0.0091 -0.0081  0.0002  0.0194
    2025-10-30 -0.0110 -0.0018  0.0067  0.0002 -0.0003
    2025-10-31  0.0040  0.0010 -0.0024  0.0002 -0.0051

     [26009 rows x 5 columns]
    ```

- Another example:
    ```python
    # To get annual data for the 5-factor model:
    data = FamaFrenchFactors(frequency="y", model=5, start_date='1970-01-01', 
                               end_date='2000-01-01' output="yes.csv")

    # Monthly 3-factor model data, since 1970 (not specifying an end date 
    #  will return data up until today).
  data = FamaFrenchFactors(model="3", frequency="m", start_date="1970-01-01") #model can be int or str
  ```


- Import only the models you need:
    ```py 
    from getfactormodels import QFactors, LiquidityFactors
    data = LiquidityFactors(frequency='m', start_date='1980-01-01')
    df = data.download()
    
    # Passing a model class without params defaults to monthly data.
    # Look! No params! Chained into .download()! zamn
    q_df = QFactors().download()
    
    # The 'q' models, and the 3-factor model of Fama-French have weekly data available
    q_df = QFactors(frequency='w', start='2025-11-01')
    print(df.tail(3))
    ```
    
    - Model classes have a `.download()` module.
    - `get_factors()` will automatically download and return the data in a df.


- All models have the parameters: `frequency`, `start_date`, `end_date`, `output_file`.
    - `FamaFrenchFactors()` has a `model=` param. Accepts: 3, 4 (Carhart), 5, 6 (default: '3')
    - `QFactors()` has a `classic=` param (default: false) for returing the classic `q4` Q-Factor model (2015).

- List of classes/models: ``HMLDevil, CarhartFactors, FamaFrenchFactors, QFactors,
  LiquidityFactors, MispricingFactors, BarillasShankenFactors, ICRFactors,
  DHSFactors``

### CLI

__This is old but should still work until redo.__

Requires ``bash >=4.2``

* You can use getfactormodels from the command line. Very simple at the moment:

  ```shell
  $ getfactormodels -h

  usage: getfactormodels [-h] -m MODEL [-f FREQ] [-s START] [-e END] [-o OUTPUT] [--no_rf] [--no_mkt]
  ```

* Retrieve the Fama-French 3-factor model data:

  ```bash
  $ getfactormodels --model ff3 --frequency M --start-date 1960-01-01 --end-date 2020-12-31 --output .csv
  ```

* Download the annual 5-factor data of Fama-French, without the RF column (using ``--no[_]rf``)

  ```shell
  $ getfactormodels -m ff5 -f Y -s 1960-01-01 -e 2020-12-31 --norf -o ~/some_dir/filename.csv
  ```

* To return the factors without the risk-free rate `RF`, or the excess market return `Mkt-RF`, columns:

  ```shell
  $ getfactormodels -m ff5 -f Y -s 1960-01-01 -e 2020-12-31 --norf --nomkt -o ~/some_dir/filename.xlsx
  ```

  ```

- ``output`` can be a filename, directory, or path. If no extension is specified, defaults to .csv (can be one of: .xlsx, .csv, .txt, .pkl, .md) 
- ``output_file`` ... not implemented properly.



## Data Availability

_This table shows each model's start date, available frequencies, and the latest datapoint if not current. The ``id`` column 
contains the shortest identifier for each model. These should all work in python and the CLI._

| `id` | Factor Model         | Start      | D            | W            | M            | Q            | Y            | End        |
|:--:|:--------------|:----------:|--------------|--------------|--------------|--------------|--------------|:----------:|
|`3`| Fama-French 3 | 1926-07-01 | $\checkmark$ | $\checkmark$ | $\checkmark$ |              | $\checkmark$ |     -       |
|`4`| Carhart 4      | 1926-11-03 | $\checkmark$ |              | $\checkmark$ |              | $\checkmark$ |     -       |
|`5`| Fama-French 5  | 1963-07-01 | $\checkmark$ |              | $\checkmark$ |              | $\checkmark$ |     -       |
|`6`| Fama-French 6 | 1963-07-01 | $\checkmark$ |              | $\checkmark$ |              | $\checkmark$ |      -      |
|`hmld`| HML $^{DEVIL}$ | 1990-07-02  | $\checkmark$ |         | $\checkmark$ |              |              |-|
|`dhs`| DHS          | 1972-07-03 | $\checkmark$ |            | $\checkmark$ |              |              | 2023-12-29 |
|`icr`| ICR           | 1970-01-31<br><sub>*Daily: 1999-05-03</sub>* | $\checkmark$ ||$\checkmark$| $\checkmark$ | | 2025-06-27 |
|`mis`| Mispricing    | 1963-01-02 | $\checkmark$ |            | $\checkmark$ |              |              | 2016-12-30 |
|`liq`| Liquidity     | 1962-08-31 |              |            | $\checkmark$ |              |              | 2024-12-31 |
|`q`<br>`q4`| $q^5$-factors<br>$q$-factors | 1967-01-03 | $\checkmark$ | $\checkmark$ | $\checkmark$ | $\checkmark$ | $\checkmark$| 2022-12-30|
|`bs`| Barillas-Shanken | 1967-01-03 | $\checkmark$ |           | $\checkmark$ |              |              | 2024-12-31 |

>[TODO]
>Docs!

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
* The first `hml_devil_factors()` retrieval is slow, because the download from aqr.com is slow. It's the only model implementing a cache—daily data expires at the end of the day, and will only re-download when the requested `end_date` exceeds the cache's latest index date. Similar for monthly, expiring at at the end of the month, and re-downloaded when next needed.
* ~~Some models aren't downloading.~~ *__Update:__ all models should be downloading.*

#### Todo
- [ ] Refactor: a complete rewrite, implementing a better interface and design patterns, dropping dependencies.
- [ ] Docs
- [ ] Every model should have an about and author/copyright info, and general disclaimer
- [ ] This README
  - [ ] Examples
- [ ] Tests
- [ ] Error handling

# IGNORE THIS
There's also the ``FactorExtractor`` class (which doesn't do much yet, it's mainly used by the CLI):

  ```python
  from getfactormodels import FactorExtractor

  fe = FactorExtractor(model='carhart', start_date='1980-01-01', end_date='1980-05-01)
  fe.get_factors()
  fe.drop_rf() 
  fe.to_file('~/carhart_factors.md')
  ```

* _The resulting ``carhart_factors.md`` file will look like this:_
    
  | date                |   Mkt-RF |     SMB |     HML |     MOM |
  |:--------------------|---------:|--------:|--------:|--------:|
  | 1980-01-31 00:00:00 |   0.0551 |  0.0162 |  0.0175 |  0.0755 |
  | 1980-02-29 00:00:00 |  -0.0122 | -0.0185 |  0.0061 |  0.0788 |
  | 1980-03-31 00:00:00 |  -0.129  | -0.0664 | -0.0101 | -0.0955 |
  | 1980-04-30 00:00:00 |   0.0397 |  0.0105 |  0.0106 | -0.0043 |

>``.drop_rf()`` will return the DataFrame without the `RF` column. You can also drop the `Mkt-RF` column with ``.drop_mkt()``

