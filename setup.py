# -*- coding: utf-8 -*-
"""Set up reportio package."""


# %% Imports
# %%% Py3 Standard
import os

# %%% 3rd Party
from setuptools import setup, find_packages


# %% Script
with open(os.path.join(os.path.dirname(__file__), 'README.md'),
          'r') as objFile:
    long_desc: str = objFile.read()
__doc__ = long_desc
short_desc: str = long_desc.split('Short Description')[1].split('\n')[1]


setup(name='reportio',
      version='0.3.2dev0',
      author='Dan Eschman',
      author_email='deschman007@gmail.com',
      url='https://github.com/deschman/reportio',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Natural Language :: English',
          'Operating System :: Microsoft :: Windows',
          'Programming Language :: Python :: 3',
          'Topic :: Software Development :: Libraries :: Python Modules'],
      python_requires='>=3.7',
      # TODO: find version dependancies for all of these
      install_requires=['abc', 'sys', 'os', 'logging', 'getpass',
                        'configparser', 'datetime', 'tempfile', 'gc',
                        'pysqlite3', 'numba', 'pytest-dbfixtures', 'pytest',
                        'pandas', 'pyarrow', 'openpyxl'],
      extras_require={'gzip_alt_processing': 'fastparquet',
                      'odbc_1': 'pyodbc',
                      'odbc_2': 'getpass',
                      'jdbc': 'jaydebeapi',
                      'mysql_support': 'mysql-connector-python',
                      'multithread_support_1': 'threading',
                      'multithread_support_2': 'multiprocessing',
                      'multithread_support_3': 'dask',
                      'progress_bar': 'tqdm'},
      description=short_desc,
      long_description=long_desc,
      long_description_content_type='text/markdown',
      packages=find_packages())
