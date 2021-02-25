# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
import os

# %%% 3rd Party
import pytest
import pandas as pd

# %%% User-Defined
from reportio.templates.simple import SimpleReport


# %% Classes
class test_SimpleReport(SimpleReport):

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

        def test__define_optional_functions():
            super()._define_optional_functions()

            def test__backup_metadata():
                super()._define_optional_functions()._backup_metadata()

            def test__delete_excel_files():
                super()._define_optional_functions()._delete_excel_files()

            def test__restore_metadata():
                super()._define_optional_functions()._restore_metadata()

    def test__delete_data_backup():
        pass

    def test__attempt_resume():
        pass

    def test__get_writer():
        pass

    def test_backup_data():
        pass

    def test_export_data():
        pass

    def test_add_query():
        pass

    def test_remove_query():
        pass

    def test_rename():
        pass

    def test_reset():
        pass

    def test_run():

        def test__process_queries():
            pass


# %% Script
if __name__ == '__main__':
    data = test_SimpleReport()
