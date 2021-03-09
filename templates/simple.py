# -*- coding: utf-8 -*-
"""
Simplified report object with less flexibility, allowing for quick development.

Example:


from reportio import SimpleReport


# Initialize report object
report = SimpleReport("Yearly Sales")

# Add queries to report object
report.add_query("Category", "SELECT * FROM CATEGORY", 'sqlite')
report.add_query("Subcategory", "SELECT * FROM SUB_CATEGORY", 'sqlite')
report.add_query("Segment", "SELECT * FROM SEGMENT", 'sqlite')

# Process and export
report.run()
"""


import os
from typing import List, Dict

import pandas as pd
from pandas.io.sql import DatabaseError
import dask
# Pending tqdm.dask module release
# from tqdm.tqdm.dask import TqdmCallback as ProgressBar

from reportio.templates import ReportTemplate
from reportio.errors import EmptyReport
from reportio.future import ProgressBar


__all__ = ['SimpleReport']


class SimpleReport(ReportTemplate):

    def __init__(self,
                 report_name: str,
                 log_location: str = '',
                 config_location: str = '',
                 metadata: pd.DataFrame = pd.DataFrame(
                     columns=['query_name',
                              'sql',
                              'db_type',
                              'connection_object',
                              'db_location'])) -> None:
        __doc__ = super().__doc__

        def _define_optional_functions(metadata: pd.DataFrame = metadata
                                       ) -> None:
            """
            For internal use. Helper function to adapt ReportTemplate.

            Parameters
            ----------
            metadata : DataFrame, supplied by script
                Contains queries to be run during report.
            """
            # Base variables
            self.metadata_file_name: str = "Metadata.xlsx"
            self.metadata: pd.DataFrame = metadata
            self.query_list: List[str] = list(self.metadata['query_name'])

            # Class specific functions
            def _backup_metadata() -> None:
                """
                For internal use. Helper function to adapt backup_data method
                to account for metadata file.
                """
                file_location: str = os.path.join(
                    self.backup_folder_location,
                    "__" + self.metadata_file_name)
                self.log("Backing up metadata to '{0}'".format(file_location),
                         'DEBUG')
                self.metadata.to_excel(file_location, index=False)
            self._backup_metadata: callable = _backup_metadata

            def _delete_excel_files(file_location: str) -> None:
                """
                For internal use. Helper function to adapt _delete_data_backup
                method to account for metadata file.

                Parameters
                ----------
                file_location : str
                    Location of excel files to be deleted.
                """
                if file_location.split('.')[-1] == 'xlsx':
                    self.log("Removing '{0}'".format(file_location))
                    os.remove(file_location)
            self._delete_excel_files: callable = _delete_excel_files

            def _restore_metadata() -> None:
                """
                For internal use. Helper function to adapt _attempt_resume
                method to account for metadata file.
                """
                file_location: str = os.path.join(self.backup_folder_location,
                                                  self.metadata_file_name)
                if os.path.isfile(file_location):
                    self.log("Restoring metadata from '{0}'".format(
                        file_location))
                    self.metadata = pd.read_excel(file_location)
                    self.query_list = list(self.metadata['query_name'])
                    # Replace NaN with None
                    self.metadata = self.metadata.where(
                        pd.notnull(self.metadata), None)
                    # Ensure names are strings
                    self.metadata = self.metadata.astype(
                        {'query_name': 'str'})
                else:
                    self.log("Metadata not found! Restarting report.", 'ERROR')
                    self._delete_data_backup()
            self._restore_metadata: callable = _restore_metadata
        _define_optional_functions()
        if log_location == '' and config_location == '':
            super().__init__(
                report_name, optional_function=_define_optional_functions)
        elif config_location == '':
            super().__init__(report_name,
                             log_location,
                             optional_function=_define_optional_functions)
        elif log_location == '':
            super().__init__(report_name,
                             config_location=config_location,
                             optional_function=_define_optional_functions)
        else:
            super().__init__(report_name,
                             log_location,
                             config_location,
                             _define_optional_functions)

    def _delete_data_backup(self) -> None:
        """
        For internal use. Deletes all gz, txt, and xlsx files in backup
        directory.
        """
        super().delete_data_backup(self._delete_excel_files)

    def _attempt_resume(self) -> List[object]:
        """
        For internal use. Checks backup folder for files that have been
        backed up today after a CRITICAL failure. Attempts to read files and
        resume report near the point of failure.

        Returns
        -------
        file_list : list
            Files found in backup.
        """
        super().attempt_resume(self._restore_metadata,
                               self._delete_excel_files)

    def _get_writer(self, report_location: str) -> pd.ExcelWriter:
        """
        For internal use. Creates ExcelWriter object to allow multiple tabs
        to be written. Deletes and creates new file at report_location.

        Parameters
        ----------
        report_location : directory
            Location of result report.

        Returns
        -------
        ExcelWriter : object
            From pandas module utilizing openpyxl as engine.
        """
        for i in [i for i in os.listdir(os.path.dirname(report_location))
                  if '.' in i]:
            if i.split('.')[0] == os.path.basename(
                    report_location).split('.')[0]:
                os.remove(report_location)
                break
        return super().get_writer(report_location)

    def backup_data(self) -> None:
        """
        Saves temporary files and metadata to backup folder in the event of
        CRICITAL failure. Backup files are utilized if script is run again the
        same day as the backup.
        """
        super().backup_data(self._backup_metadata)

    def export_data(self,
                    file: object,
                    report_location: str = '',
                    sheet: str = '',
                    export_locations: List[str] = [],
                    excel_writer: pd.ExcelWriter = None) -> List[str]:
        """
        Export report data to report file. Will change file type to CSV as
        needed.

        Parameters
        ----------
        file : object
            Must be parquet file. Contains report data for export.
        report_location : str, optional
            Name of result file. File extension will be overwritten. Default is
            from config file.
        sheet : str, optional
            Name of excel sheet. Will append to file name if data is too large
            for Excel.
        export_locations : list, optional
            All directories already used for export
        excel_writer : pandas.ExcelWriter, optional
            From pandas module. Used to append additional tabs to existing
            document.

        Returns
        -------
        export_locations: list
            Final directory used for export appended to export_locations key
            word arg.
        """
        if report_location == '':
            report_location = self.config['REPORT']['export_to']
        export_locations.append(
            super().export_data(file, report_location, sheet, excel_writer))
        return export_locations

    def add_query(self,
                  query_name: str,
                  sql: str,
                  db_type: str = '',
                  connection: object = None,
                  db_location: str = '') -> None:
        """
        Add query to list to be run upon execution. Alternative to setting
        metadata upon initializing. Will reset .query_list after addition.

        Parameters
        ----------
        query_name : str
            Name for dataset. Cannot use '__', for file path.
        sql : str
            SQL statement as a string. Should be formatted for desired database
        db_type : str, optional
            Either db_type or connection must be provided to connect to
            database.
        connection : object, optional
            If not provided, report will attempt to connect using string in
            config vile. Either db_type or connection must be provided
            to connect to database.
        db_location : str, optional
             Database location. Must only be provided if db_type is 'sqlite'.
        """
        if query_name not in self.query_list:
            self.log("""Adding row to metadata with:
                         query_name: {0}
                         sql: {1}
                         db_type: {2}
                         connection: {3}
                         db_location: {4}""".format(query_name,
                                                    sql,
                                                    db_type,
                                                    connection,
                                                    db_location),
                     'DEBUG')
            self.metadata = self.metadata.append(pd.DataFrame(
                [[query_name, sql, db_type, connection, db_location]],
                columns=['query_name',
                         'sql',
                         'db_type',
                         'connection',
                         'db_location']),
                ignore_index=True)
            self.query_list = list(self.metadata['query_name'])
        else:
            self.log(
                "Query with name '{0}' already exists. Query not added".format(
                    query_name), 'WARNING')

    def remove_query(self, query_name: str) -> None:
        """
        Remove query from list to be run upon execution. See '.queries' for
        a list of all query names. Will reset .query_list after removal.

        Parameters
        ----------
        query_name : str
            Name of query.
        """
        self.log("Removing query '{0}' from list".format(query_name))
        self.metadata = self.metadata.drop(
            self.metadata.loc[self.metadata['query_name'] == query_name].index)
        self.query_list = list(self.metadata['query_name'])

    def rename(self, report_name: str) -> None:
        """
        Will rename report, changing file output.

        Parameters
        ----------
        query_name: str
            Name of report
        """
        self.log("Renaming report to '{0}'".format(report_name))
        self.report_name = report_name
        self.config['REPORT']['report_name'] = report_name
        with open(self.config, 'w') as file:
            self.config.write(file)

    def reset(self) -> None:
        """
        Clear metadata, delete backup data, and reinitialize
        """
        self.log("Reseting report")
        self._delete_data_backup()
        self.__init__(
            self.report_name, self.log_location, self.config_location)

    def run(self, multithread: bool = True, export_locations: List[str] = []
            ) -> List[str]:
        """
        Will run all queries included in report then export to individual
        tabs named for each dataset. On CRITICAL failure, will backup data and
        attempt to resume report on next run.

        Parameters
        ----------
        multithread : bool, default True
            Will utilize multithreading if True. Single thread is useful for
            debug.

        Returns
        -------
        export_locations : list
            All unique directories where report was exported.
        """

        def _process_queries(multithread=True,
                             export_locations: List[str] = []) -> List[str]:
            """
            For internal use. Multithread section of run method.

            Parameters
            ----------
            export_locations : list, optional
                Runs all queries in report. Utilizes single thread.

            Returns
            -------
            export_locations : list
                All unique directories where report was exported.
            """
            # Loop metadata dataframe
            for _, row in self.metadata.iterrows():
                # Schedule data retrieval
                data = dask.delayed(self.get_data)(
                    str(row['query_name']),
                    str(row['sql']),
                    str(row['db_type']),
                    row['connection_object'],
                    str(row['db_location']))[0]
                export_locations = dask.delayed(self.export_data)(
                    data, self.config['REPORT']['export_to'],
                    str(row['query_name']),
                    export_locations,
                    self._obj_writer)
            with ProgressBar():
                if multithread:
                    try:
                        self.log("Running with multithreading")
                        export_locations = list(set(
                            export_locations.compute()))
                    except DatabaseError:
                        self.log("Failed to multithread, reverting to single")
                        export_locations = list(set(
                            export_locations.compute(
                                scheduler='single-threaded')))

                else:
                    self.log("Running on single thread")
                    export_locations = list(set(
                        export_locations.compute(scheduler='single-threaded')))

            return export_locations

        try:
            # Initialize new Excel writer
            self._obj_writer: pd.ExcelWriter = self._get_writer(
                self.config['REPORT']['export_to'])
            # Check for queries
            if len(self.metadata.index) > 0:
                export_locations = _process_queries(multithread,
                                                    export_locations)
                # Rearrange sheets to match query order if needed
                try:
                    if len(self._obj_writer.book._sheets) > 1:
                        sheet_order: Dict[str, object] = dict(
                            (ws.title, ws) for ws in
                            self._obj_writer.book._sheets)
                        self._obj_writer.book._sheets = [sheet_order.get(
                            str(i)) for i in self.query_list]
                        self._obj_writer.save()
                        self._obj_writer.close()
                except (IndexError, AttributeError):
                    pass
                # Inform user of export directories used
                self.log("Directory List:")
                for location in set(export_locations):
                    self.log("'" + location + "'")
                self._delete_data_backup()

                return export_locations
            else:
                raise EmptyReport
        except Exception:
            self.log("See log for debug details", 'CRITICAL')
            self.backup_data()
            self.log("Backup successful")
            input("PRESS ENTER KEY TO QUIT")
            self.log("QUITTING")
