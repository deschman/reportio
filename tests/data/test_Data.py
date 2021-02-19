# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
import os
import sqlite3
import time

# %%% 3rd Party
import pytest
import pandas as pd
import dask.distributed as dd

# %%% User-Defined
from reportio.data import Data


# %% Variables
client = dd.Client(processes=False)
file_folder = os.path.dirname(__file__)
root_dir = os.path.dirname(os.path.dirname(file_folder))


# %% Classes
class test_Data(Data):

    # %%% Functions
    # %%%% Private
    def __init__(self,
                 dataset_name: str = 'test',
                 log: callable = print,
                 client: dd.Client = client,
                 temporary_folder_location: str = os.path.join(file_folder,
                                                               '_temp_files'),
                 backup_folder_location: str = os.path.join(file_folder,
                                                            '_backup'),
                 sql: str = "SELECT * FROM CATEGORY",
                 connection: object = sqlite3.connect(
                     os.path.join(root_dir, 'sample.db'),
                     check_same_thread=False)) -> None:
        super().__init__(dataset_name,
                         log,
                         client,
                         temporary_folder_location,
                         backup_folder_location,
                         sql,
                         connection)

    # %%%% Public
    # This fails due to https://github.com/dask/distributed/issues/4464
    # TODO: work around above bug
    def test__get_data(self) -> None:
        super()._get_data()
        while isinstance(self._DataFrame, dd.Future):
            time.sleep(1)
        assert isinstance(self._DataFrame, pd.DataFrame)

    def test__get_temp_file(self) -> None:
        super()._get_temp_file()
        while isinstance(self._File, dd.Future):
            time.sleep(1)
        assert isinstance(self._File, object)

    def test_set_dataset_name(self) -> None:
        pass

    def test_set_sql(self) -> None:
        pass

    def test_set_connection(self) -> None:
        pass

    def test_set_DataFrame(self) -> None:
        pass

    def test_join(self) -> None:
        pass

    def test_cross_query(self) -> None:
        pass

    def test_merge_files(self) -> None:
        pass


# %% Script
if __name__ == '__main__':
    data = test_Data()
    data.test__get_data()
    data.test__get_temp_file()
