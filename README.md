<a name="readme-top"></a>

# getfactormodels

![Python 3.11](https://img.shields.io/badge/Python-3.7+-306998.svg?logo=python&logoColor=ffde57&style=flat-square) ![PyPI - Version](https://img.shields.io/pypi/v/getfactormodels?style=flat-square&label=PyPI)
![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square)


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

`getfactormodels` requires Python ``>=3.7``

* The easiest way to install getfactormodels is via pip:
  
  ```shell
  $ pip install getfactormodels
  ```

## Usage

>[!IMPORTANT]
>![PyPI - Status](https://img.shields.io/pypi/status/getfactormodels?style=flat-square)
>
>``getfactormodels`` is new. It was released on December 20, 2023. Don't rely on it for anything.

After installation, import and call the ``get_factors()`` function with the ``model`` and ``frequency`` params:

* For example, retrieving the monthly ${q}^{5}$ factor model:
  
  ```python
   import getfactormodels
  
   data = getfactormodels.get_factors(model='q', frequency='m')
  ```

  > _Trimmed output:_

  ```txt
  > print(data)
                Mkt-RF      R_ME      R_IA     R_ROE      R_EG        RF
  date                                                                  
  1967-01-03  0.000778  0.004944  0.001437 -0.007118 -0.008563  0.000187
  1967-01-04  0.001667 -0.003487 -0.000631 -0.002044 -0.000295  0.000187
  1967-01-05  0.012990  0.004412 -0.005688  0.000838 -0.003075  0.000187
  1967-01-06  0.007230  0.006669  0.008897  0.003603  0.002669  0.000187
  1967-01-09  0.008439  0.006315  0.000331  0.004949  0.002979  0.000187
  ...              ...       ...       ...       ...       ...       ...
  2022-12-23  0.005113 -0.001045  0.004000  0.010484  0.003852  0.000161
  2022-12-27 -0.005076 -0.001407  0.010190  0.009206  0.003908  0.000161
  2022-12-28 -0.012344 -0.004354  0.000133 -0.010457 -0.004953  0.000161
  2022-12-29  0.018699  0.008568 -0.008801 -0.012686 -0.002162  0.000161
  2022-12-30 -0.002169  0.001840  0.001011 -0.004151 -0.003282  0.000161

  [14096 rows x 6 columns]
  ```

* Retrieving the daily data for the Fama-French 3-factor model, since `start_date`:

  ```python
  import getfactormodels as gfm

  df = gfm.get_factors(model='ff3', frequency='d', start_date=`2006-01-01`)
  ```

* Retrieving data for Stambaugh and Yuan's monthly *Mispricing* factors, between `start_date` and `end_date`, and saving the data to a file:

  ```python
  import getfactormodels as gfm
  
  df = gfm.get_factors(model='mispricing', start_date='1970-01-01', end_date=1999-12-31, output='mispricing_factors.csv')
  ```

  >``output`` can be a filename, directory, or path. If no extension is specified, defaults to .csv (can be one of: .xlsx, .csv, .txt, .pkl, .md)

You can import only the models that you need:

* For example, to import only the *ICR* and *q-factor* models:

  ```python
  from getfactormodels import icr_factors, q_factors

  # Passing a model function without params defaults to monthly data.
  df = icr_factors()

  # The 'q' models, and the 3-factor model of Fama-French have weekly data available:
  df = q_factors(frequency="W", start_date="1992-01-01, output='.xlsx')
  ```

  >``output`` allows just a file extension (with the `.`, else it'll be passed as a filename).

* When using `ff_factors()`, specify an additional `model` parameter (**this might be changed**):
  
  ```python
  # To get annual data for the 5-factor model:
  data = ff_factors(model="5", frequency="Y", output=".xlsx")

  # Daily 3-factor model data, since 1970 (not specifying an end date
  # will return data up until today):
  data = ff_factors(model="3", frequency="D", start_date="1970-01-01")
  ```

There's also a ``FactorExtractor`` class (which doesn't do much yet, it's mainly used by the CLI; lots to do):

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

>``.drop_rf()`` will return the DataFrame without the `RF` column. You can also drop the "Mkt-RF" column with ``.drop_mkt()``

### CLI

``bash >=4.2``

* You can also use getfactormodels from the command line. It's very barebones, here's the `-h`:

  ```shell
  $ getfactormodels -h

  usage: getfactormodels [-h] -m MODEL [-f FREQ] [-s START] [-e END] [-o OUTPUT] [--no_rf] [--no_mkt]
  ```

* An example of how to use the CLI to retrieve the Fama-French 3-factor model data:

  ```shell
  $ getfactormodels --model ff3 --frequency M --start-date 1960-01-01 --end-date 2020-12-31 --output ".csv"
  ```

* Here's another example that retrieves the annual Fama-French 5-factor data without the RF column (using ``--no_rf``)

  ```shell
  $ getfactormodels -m ff5 -f Y -s 1960-01-01 -e 2020-12-31 --no_rf -o ~/some_dir/filename.xlsx
  ```
* To return the factors without the risk-free rate `RF`, or the excess market return `Mkt-RF`, columns:
  
## Data Availability

>[TODO]

## References
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
* Z. He, "Intermediary Capital Ratio and Risk Factor" dataset, University of Chicago. 
[Link](https://voices.uchicago.edu/zhiguohe/data-and-empirical-patterns/intermediary-capital-ratio-and-risk-factor/)
* K. Hou, G. Xue, R. Zhang, "The Hou-Xue-Zhang q-factors data library," at global-q.org.
[Link](http://global-q.org/factors.html)
* AQR Capital Management's Data Sets.
* Lin Sun, DHS Behavioural factors [Link](https://sites.google.com/view/linsunhome)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License

![License](https://img.shields.io/badge/MIT-blue?style=for-the-badge&logo=license&colorA=grey&colorB=blue)

*The code in this project is released under the [MIT License]().*

[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat-square&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Ruff](https://img.shields.io/badge/-ruff-%23261230?style=flat-square&logo=ruff&logoColor=d7ff64)](https://simpleicons.org/?q=ruff)
---

#### Known issues

* The first `hml_devil_factors()` retrieval is slow, because the download from aqr.com is slow. It's the only model, so far, implementing a cache—daily data expires at the end of the day and is only re-downloaded when the requested`end_date` exceeds the file's last index date. Similar for monthly, expiring at EOM and re-downloaded when needed.

#### Todo

- [ ] Docs
  - [ ] Examples
- [ ] Tests
- [ ] Error handling
