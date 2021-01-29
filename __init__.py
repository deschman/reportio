# -*- coding: utf-8 -*-


from typing import Tuple, List


# Let users know if they're missing any of our hard dependencies
hard_dependencies: Tuple[str] = ('abc',
                                 'sys',
                                 'os',
                                 'logging',
                                 'threading',
                                 'configparser',
                                 'datetime',
                                 'tempfile',
                                 'gc',
                                 'sqlite3',
                                 'pytest',
                                 'numba',
                                 'pandas',
                                 'pyarrow',
                                 'openpyxl')
missing_dependencies: List[str] = []

for dependency in hard_dependencies:
    try:
        __import__(dependency)
    except ImportError as e:
        missing_dependencies.append(f"{dependency}: {e}")

if missing_dependencies:
    raise ImportError(
        "Unable to import required dependencies:\n" + "\n".join(
            missing_dependencies))

# Pending tqdm.dask module release
# from tqdm.tqdm.dask import TqdmCallback as ProgressBar

from reporting.templates import ReportTemplate
from reporting.data import Data
from reporting.templates.simple import SimpleReport
from reporting import logging
from reporting.errors import *
from reporting.future.tqdm.dask import TqdmCallback as ProgressBar

__all__ = ['ProgressBar',
           'ReportTemplate',
           'Data',
           'SimpleReport',
           'logging',
           'ReportError',
           'LogError',
           'ConfigError',
           'ReportNameError',
           'DBConnectionError',
           'UnexpectedDbType',
           'DatasetNameError',
           'EmptyReport']

# TODO: implement email delivery for outlook (and gmail?)
