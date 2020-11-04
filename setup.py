# -*- coding: utf-8 -*-


from setuptools import setup, find_packages
from os.path import dirname as os_path_dirname
from os.path import join as os_path_join


with open(os_path_join(os_path_dirname(__file__), 'README.md'),
          'r') as objFile:
    strLongDesc = objFile.read()
strShortDesc = strLongDesc.split('Short Description')[1].split('\n')[1]


setup(name='reporting',
      version='0.2.0a0',
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
      # TODO: idenfity version requirements
      python_requires='~=3.7',
      install_requires=['abc', 'sys', 'os', 'logging', 'threading',
                        'multiprocessing', 'configparser', 'datetime',
                        'tempfile', 'gc', 'pytest', 'pytest_dbfixtures',
                        'pyodbc', 'pandas', 'pyarrow', 'fastparquet', 'dask',
                        'sqlite3', 'openpyxl', 'tqdm'],
      description=strShortDesc,
      long_description=strLongDesc,
      long_description_content_type='text/markdown',
      packages=find_packages())
