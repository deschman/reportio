# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
import os
from typing import List

# %%% 3rd Party
import pytest
import pandas as pd

from reportio.templates.simple import SimpleReport


class test_SimpleReport(SimpleReport):

    # %%% Functions
    # %%%% Private
    def __init__(self,
                 report_name: str = 'test',
                 log_location: str = os.path.join(
                     os.path.dirname(__file__), 'simple_log.txt'),
                 config_location: str = os.path.join(
                     os.path.dirname(__file__), 'simple_config.txt'),
                 metadata: pd.DataFrame = pd.DataFrame(
                     columns=['query_name',
                              'sql',
                              'db_type',
                              'connection_object',
                              'db_location'])) -> None:
        super().__init__(report_name, log_location, config_location, metadata)

        # %%%%% Functions
        def test__define_optional_functions(self):
            # %%%%%% Functions
            def test__backup_metadata():
                super()._define_optional_functions()._backup_metadata()
                assert True

            def test__delete_excel_files():
                super()._define_optional_functions()._delete_excel_files()
                assert True

            def test__restore_metadata():
                super()._define_optional_functions()._restore_metadata()
                assert True

            # %%%%%% Script
            super()._define_optional_functions()
            assert True

    # %%%% Public
    def test_backup_data(self):
        super().backup_data()
        temp_files: List[str] = [file for file in
                                 os.listdir(self.temp_files_location)
                                 if file.split('.')[-1] == 'gz']
        backup_files: List[str] = [file for file in
                                   os.listdir(self.backup_folder_location)
                                   if file.split('.')[-1] == 'gz']
        assert temp_files == backup_files

    def test_export_data(self, file: object = None, report_location: str = ''):
        file = self.file
        self.location = super().export_data(file, report_location)
        assert isinstance(self.loction, str)

    def test_add_query(self):
        metadata_length_before: List[str] = len(self.metadata)
        self.add_query()
        metadata_length_after: List[str] = len(self.metadata)
        assert metadata_length_before + 1 == metadata_length_after

    def test_remove_query(self, query_name: str = ''):
        metadata_length_before: List[str] = len(self.metadata)
        self.remove_query(query_name)
        metadata_length_after: List[str] = len(self.metadata)
        assert metadata_length_before - 1 == metadata_length_after

    def test_rename(self, name: str = 'test_renamed'):
        self.rename(name)
        assert self.name == name

    def test_reset(self):
        self.reset()
        assert True

    def test_run(self):
        # %%%% Functions
        def test__process_queries():
            pass
        # %%%% Script
        assert True


# %% Script
if __name__ == '__main__':
    data = test_SimpleReport()
