# -*- coding: utf-8 -*-


# Let users know if they're missing any of our hard dependencies
# TODO: find version dependancies for all of these
# TODO: program 'soft' dependancies list (fastparquet, others?)
# TODO: fix pytest_dbfixtures dependancy issue
hard_dependencies = ('abc', 'sys', 'os', 'logging', 'threading',
                     'multiprocessing', 'configparser', 'datetime',
                     'tempfile', 'gc', 'pytest',
                     'pyodbc', 'pandas', 'pyarrow', 'dask',
                     'sqlite3', 'openpyxl', 'tqdm')
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

from .templates import ReportTemplate
from .templates.simple import SimpleReport
from .logging import *
from .errors import *
from .future.progress import ProgressBar

# TODO: implement email delivery for outlook (and gmail?)
