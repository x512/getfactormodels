.. getfactormodels documentation master file, created by
   sphinx-quickstart on Sun Dec 24 22:19:35 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

getfactormodels
=====================================

.. toctree::
   :maxdepth: 3
   :caption: Contents:

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. currentmodule:: getfactormodels

This is the documentation for the `getfactormodels` package. This package provides functions for retrieving and processing multi-factor model data.

.. autofunction:: getfactormodels.get_factors

Available Models [page]
----------------

The following models are currently implemented:

.. automodule:: getfactormodels
   :members:
   :exclude-members: FactorExtractor, get_factors
   :undoc-members:
   :show-inheritance:

FactorExtractor
----------------

.. autoclass:: getfactormodels.FactorExtractor
   :members:
   :exclude-members: __init__
   :undoc-members:
   :show-inheritance:

CLI
-------------
The `getfactormodels` package also provides a command line interface (CLI) for retrieving and processing multi-factor model data. The CLI can be accessed by running `getfactormodels` from the command line. The CLI provides the following commands:

.. code-block:: bash

    usage: getfactormodels [-h] [-m MODEL] [-f FREQ] [-s START] [-e END] [-o OUTPUT] [--no_rf] [--no_mkt]

    optional arguments:
      -h, --help            show this help message and exit
      -m MODEL, --model MODEL
                            The model to use.
      -f FREQ, --freq FREQ, --frequency FREQ
                            The frequency of the data. Valid options are D, W, M, Q, A.
      -s START, --start START
                            The start date for the data.
      -e END, --end END     The end date for the data.
      -o OUTPUT, --output OUTPUT
                            The file to save the data to.
      --no_rf, --no-rf, --norf
                            Drop the RF column from the DataFrame.
      --no_mkt, --no-mkt, --nomkt
                            Drop the Mkt-RF column from the DataFrame.


Example:
   Using the CLI::
      
      $ getfactormodels -m 3 -f M -s 1961-01-01 -e 1990-12-31
      
      $ getfactormodels --model icr --frequency M --end 1990-12-31 --no_rf -o '~/icr.csv'
