# -*- coding: utf-8 -*-


from setuptools import setup, find_packages
import os


with open(os.path.join(os.path.dirname(__file__), 'README.md'),
          'r') as objFile:
    strLongDesc: str = objFile.read()
strShortDesc: str = strLongDesc.split('Short Description')[1].split('\n')[1]


setup(name='reporting',
      version='0.3.0dev0',
      author='Dan Eschman',
      author_email='deschman007@gmail.com',
      url='https://github.com/deschman/reporting',
      classifiers=[
          'Natural Language :: English',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Programming Language :: Python :: 3',
          'Operating System :: Microsoft :: Windows',
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)'],
      python_requires='~=3.7',
      # TODO: find version dependancies for all of these
      # TODO: program 'soft' dependancies
      install_requires=['abc', 'sys', 'os', 'logging', 'configparser',
                        'datetime', 'tempfile', 'gc', 'pysqlite3', 'numba',
                        'pytest-dbfixtures', 'pytest', 'pandas', 'pyarrow',
                        'openpyxl'],
      extras_require={'gzip_alt_processing': 'fastparquet',
                      'odbc': 'pyodbc',
                      'mysql_support': 'mysql-connector-python',
                      'multithread_support_1': 'threading',
                      'multithread_support_2': 'multiprocessing',
                      'multithread_support_3': 'dask',
                      'progress_bar': 'tqdm'},
      description=strShortDesc,
      long_description=strLongDesc,
      long_description_content_type='text/markdown',
      packages=find_packages())
