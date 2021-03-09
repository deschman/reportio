# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
import os
from typing import Dict, List
import sqlite3
import tempfile

# %%% 3rd Party
import pytest
import dask.distributed as dd
import pandas as pd

# %%% User-Defined
from reportio.templates import ReportTemplate, write_config


def test_write_config(self):
    pass


class test_ReportTemplate(ReportTemplate):
    # %%% Functions
    # %%%% Private
    def __init__(self,
                 report_name: str = 'test',
                 log_location: str = os.path.join(
                     os.path.dirname(__file__), 'simple_log.txt'),
                 config_location: str = os.path.join(
                     os.path.dirname(__file__), 'simple_config.txt'),
                 connection_dictionary: Dict[str, object] = {},
                 client: dd.Client = None,
                 optional_function: callable = None) -> None:
        super().__init__(report_name,
                         log_location,
                         config_location,
                         connection_dictionary,
                         client,
                         optional_function)

    # %%%% Public
    def run(self):
        self.file = self.get_data('test_data',
                                  "SELECT * FROM CATEGORY",
                                  'sqlite')[0]
        self.export_data(self.file, self.config['REPORT']['export_to'])

    def test_get_connection(self,
                            database_name: str = 'test',
                            connection_string: str = os.path.join(
                                os.path.dirname(os.path.dirname(
                                    os.path.dirname(__file__))),
                                'sample.db'),
                            connection_type: str = 'sqlite') -> None:
        self.connection = super().get_connection(
            database_name,
            connection_string,
            connection_type)
        assert isinstance(self.connection, sqlite3.Connection)

    def test_get_writer(self,
                        report_location: str = os.path.join(
                            os.path.dirname(__file__), 'test.xlsx')) -> None:
        self.writer = super().get_writer(report_location)
        assert isinstance(self.writer, pd.ExcelWriter)

    def test_get_data(self,
                      data_name: str = 'test',
                      sql: str = "SELECT * FROM CATEGORY",
                      connection_object: object = None):
        if not hasattr(self, 'connection'):
            self.test_get_connection()
        connection_object = self.connection
        self.data = super().get_data(
            data_name, sql, connection_object=connection_object)[0]
        assert isinstance(self.data, pd.DataFrame)

    def test_get_temp_file(self,
                           file_name: str,
                           data: pd.DataFrame,
                           folder_location: str) -> None:
        self.file = super().get_temp_file(file_name, data, folder_location)
        assert isinstance(self.file, tempfile.TemporaryFile)

    def test_export_data(self,
                         file: object = None,
                         report_location: str = '',
                         sheet: str = '',
                         excel_writer: str = ''):
        file = self.file
        self.location = super().export_data(
            file, report_location, sheet, excel_writer)
        assert isinstance(self.loction, str)

    def test_run(self):
        self.run()
        assert True

    def test_backup_data(self):
        super().backup_data()
        temp_files: List[str] = [file for file in
                                 os.listdir(self.temp_files_location)
                                 if file.split('.')[-1] == 'gz']
        backup_files: List[str] = [file for file in
                                   os.listdir(self.backup_folder_location)
                                   if file.split('.')[-1] == 'gz']
        assert temp_files == backup_files

    def test_attempt_resume(self):
        backed_up_files: List[str] = super().attempt_resume()
        backup_files: List[str] = [file for file in
                                   os.listdir(self.backup_folder_location)
                                   if file.split('.')[-1] == 'gz']
        assert backed_up_files == backup_files

    def test_delete_data_backup(self):
        super().delete_data_backup()
        backup_files: List[str] = [file for file in
                                   os.listdir(self.backup_folder_location)
                                   if file.split('.')[-1] == 'gz']
        assert len(backup_files) == 0


# %% Functions
def test_write_config(self):
    write_config()
    assert True
