<a name="readme-top"></a>

# getfactormodels

![Python 3.11](https://img.shields.io/badge/Python-3.7+-306998.svg?logo=python&logoColor=ffde57&style=flat-square) ![PyPI - Version](https://img.shields.io/pypi/v/getfactormodels?style=flat-square&label=PyPI)


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


_Thanks to: Kenneth French, Robert Stambaugh, Lin Sun, Zhiguo He, AQR Capital Management (AQR.com) and Hou, Xue and Zhang (global-q.org), for their research and for the datasets they publically provide._


## Installation

`getfactormodels` requires Python ``>=3.7``

* Install with pip:
  ```shell
  $ pip install getfactormodels   
  ```

## Usage

#### Python

After installing, import ``getfactormodels`` and call ``get_factors()`` with the ``model`` and ``frequency`` parameters. Optionally, specify a ``start_date`` and ``end_date``
* For example, to retrieve the daily q-factor model data:

    ```py
    import getfactormodels as getfactormodels
    
    df = getfactormodels.get_factors(model='q', frequency='d')
    ```
    > _Trimmed output:_
    ```txt
    > df
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

    * or, retreive the monthly liquidity factors of Pastor and Stambaugh for the 1990s:

    ```py
      import getfactormodels as getfactormodels
    
      df = getfactormodels.get_factors(model='liquidity', frequency='m', start_date='1990-01-01', end_date='1999-12-31')
    ```
    > If you don't have time to type `liquidity`, type `liq`, or `ps`--there's a handy regex.

    * or, saving the monthly 3-factor model of Fama & French to a file:

    ```py
      import getfactormodels as gfm

      df = gfm.get_factors(model='ff3', frequency='m', output="ff3_data.csv")
    ```
     >The output parameter accepts a filename, path or directory, and can be one of csv, md, txt, xlsx, pkl.


You can also import just the models that you need. 

* For example, to import only the *ICR* and *q*-factor models: 

    ```py
      from getfactormodels import icr_factors, q_factors

      # Passing a model function with no params defaults to monthly.
      df = icr_factors()

      # The 'q' models, and the 3-factor model of Fama-French also have weekly data.
      df = q_factors(frequency="W", start_date="1992-01-01)
    ```

* If using ``ff_factors()``, then an additional ``model`` parameter should be specified:

    ```py
    from getfactormodels import ff_factors

    # To get annual data for the 5-factor model:
    data = ff_factors(model="5", frequency="Y", output=".xlsx")

    # Daily 3-factor model data, since 1970 (not specifying an end date
    # will return data up until today):
    data = ff_factors(model="3", frequency="D", start_date="1970-01-01")
    ```
    > Output allows just an extension to be specified.

* or import all the models:

  ```py
    from getfactormodels import models
  ```

* There's also the `FactorExtractor` class that the CLI uses (it doesn't really do a whole lot yet):

  ```python
    from getfactormodels import FactorExtractor

    fe = FactorExtractor(model='carhart', frequency='m', start_date='1980-01-01', end_date='1980-05-01')
    fe.get_factors()
    fe.to_file('carhart_factors.md')
    ```

  * _The resulting ``carhart_factors.md`` file will look like this:_
    
    | date                |   Mkt-RF |     SMB |     HML |     MOM |     RF |
    |:--------------------|---------:|--------:|--------:|--------:|-------:|
    | 1980-01-31 00:00:00 |   0.0551 |  0.0162 |  0.0175 |  0.0755 | 0.008  |
    | 1980-02-29 00:00:00 |  -0.0122 | -0.0185 |  0.0061 |  0.0788 | 0.0089 |
    | 1980-03-31 00:00:00 |  -0.129  | -0.0664 | -0.0101 | -0.0955 | 0.0121 |
    | 1980-04-30 00:00:00 |   0.0397 |  0.0105 |  0.0106 | -0.0043 | 0.0126 |


#### Using the CLI
* You can also use getfactormodels from the command line.

    ```bash
    $ getfactormodels -h

    usage: getfactormodels [-h] -m MODEL [-f FREQ] [-s START] [-e END] [-o OUTPUT] [--no_rf]
    ```

* An example of how to use the CLI to retrieve the Fama-French 3-factor model data:
    ```bash
       getfactormodels --model ff3 --frequency M --start-date 1960-01-01 --end-date 2020-12-31 --output "filename.csv"
    ```
    > Accepted file extensions are .csv, .txt, .xlsx, and .md. If no extension is given, the output file will be .csv. The --output flag allows a filename, filepath or a directory. If only an extension is provided (including the . else it'll be passed as a filename), a name will be generated.
    
* Here's another example that retrieves the annual Fama-French 5-factor data without the RF column:

  ```sh
    getfactormodels -m 5 -f Y -s 1960-01-01 -e 2020-12-31 --no_rf -o ~/some_dir/filename.xlsx
  ```
    > `--no_rf` will return the factor model without an RF column.

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
