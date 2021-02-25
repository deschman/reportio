# -*- coding: utf-8 -*-


from setuptools import setup, find_packages
import os


with open(os.path.join(os.path.dirname(__file__), 'README.md'),
          'r') as objFile:
    long_desc: str = objFile.read()
short_desc: str = long_desc.split('Short Description')[1].split('\n')[1]


setup(name='reportio',
      version='0.3.4',
      author='Dan Eschman',
      author_email='deschman007@gmail.com',
      url='https://github.com/deschman/reportio',
      classifiers=[
          'Natural Language :: English',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Programming Language :: Python :: 3',
          'Operating System :: Microsoft :: Windows',
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'],
      python_requires='~=3.7',
      # TODO: find version dependancies for all of these
      install_requires=['pysqlite3', 'numba', 'pytest-dbfixtures',
                        'pandas', 'pyarrow', 'openpyxl'],
      extras_require={'gzip_alt_processing': 'fastparquet',
                      'odbc_1': 'pyodbc',
                      'jdbc': 'jaydebeapi',
                      'mysql_support': 'mysql-connector-python',
                      'multithread_support_3': 'dask',
                      'progress_bar': 'tqdm'},
      description=short_desc,
      long_description=long_desc,
      long_description_content_type='text/markdown',
      packages=find_packages())
