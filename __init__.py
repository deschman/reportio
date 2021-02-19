# -*- coding: utf-8 -*-
"""
A package containing templates for reporting with Python.

Copyright (C) 2021 Dan Eschman

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""


# %% Imports
import os
from typing import Tuple, List


# %% Script
with open(os.path.join(os.path.dirname(__file__), 'README.md'),
          'r') as objFile:
    long_desc: str = objFile.read()
__doc__ = long_desc

# Let users know if they're missing any of our hard dependencies
hard_dependencies: Tuple[str] = ('abc',
                                 'sys',
                                 'os',
                                 'logging',
                                 'getpass',
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

from reportio.templates import ReportTemplate
from reportio.data import Data
from reportio.templates.simple import SimpleReport
from reportio import logging
from reportio.errors import *
from reportio.future import ProgressBar

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
