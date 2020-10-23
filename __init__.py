# -*- coding: utf-8 -*-


# TODO: find version dependancies for all of these
# TODO: determine 'soft' dependancies
# Let users know if they're missing any of our hard dependencies
hard_dependencies = ('abc', 'sys', 'os', 'logging', 'threading',
                     'multiprocessing', 'configparser', 'datetime', 'tempfile',
                     'gc', 'pyodbc', 'pandas', 'pyarrow', 'fastparquet',
                     'dask', 'sqlite3', 'openpyxl', 'tqdm')
missing_dependencies = []

for dependency in hard_dependencies:
    try:
        __import__(dependency)
    except ImportError as e:
        missing_dependencies.append(f"{dependency}: {e}")

if missing_dependencies:
    raise ImportError(
        "Unable to import required dependencies:\n" + "\n".join(
            missing_dependencies))

from mymodules.reporting.templates import *
from mymodules.reporting.errors import *
from mymodules.reporting.future.progress import ProgressBar

# TODO: implement email delivery for outlook (and gmail?)
# module level doc-string
__doc__ = """
reporting - a package containing template classes for reporting with Python
===============================================================================

**reporting** is a Python package providing template classes in an effort to
speed up report building for the Pricing department at O'Reilly Auto Parts. It
aims to provide users with an API for interacting with various data sources and
end-user file types.

Limitations:
     - User should have solid grasp Python and object oriented programming
     - User should be familiar with available data sources and structures
     - User must be prepared to interact with data sources using SQL
     - User must deliver reports outside the scope of this module
"""
__version__ = '0.2a0'
