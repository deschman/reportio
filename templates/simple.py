# -*- coding: utf-8 -*-


from traceback import format_stack as tb_format_stack  # error printing in log
from os import remove as os_remove  # delete backup files
from os.path import join as os_path_join  # path joinging
from os.path import isfile as os_path_isfile  # file validation

import pandas as pd  # assorted data wrangling and IO
from dask import delayed as dd  # multithreading

from ..templates import ReportTemplate
from ..errors import EmptyReport
from ..future.progress import ProgressBar
# TODO: implement decLog
# from ..logging import decLog as decLog


class SimpleReport(ReportTemplate):

    def backupData(self):
        """Saves temporary files and metadata to backup folder in the event of
        CRICITAL failure. Backup files are utilized if script is run again the
        same day as the backup."""
        super().backupData(self._backupMetadata)

    def _delDataBackup(self):
        """For internal use. Deletes all gz, txt, and xlsx files in backup
        directory."""
        super()._delDataBackup(self._delExcelFiles)

    def _attemptResume(self):
        """For internal use. Checks backup folder for files that have been
        backed up today after a CRITICAL failure. Attempts to read files and
        resume report near the point of failure.

        Returns
        -------
        lstFiles : list
            Files found in backup."""
        super()._attemptResume(self._restoreMetadata, self._delExcelFiles)

    def __init__(self, strName, dirLog=None, dirConfig=None,
                 dfMetadata=pd.DataFrame(columns=['strName', 'strSQL',
                                                  'strDbType', 'objCnxn',
                                                  'dirDb'])):
        """Will connect, query, and export data for each row in dfMetadata.
        Metadata may be added using .addQuery after creation of the
        SimpleReport object.

        Parameters
        ----------
        strName : string
            Name of report.
        dirLog : directory, default is in user folder
            File where script will log processes. Will create new log file if
            directory does not exist.
        dirConfig : directory, default is template config file
            Location of config file.
        dfMetadata : DataFrame, optional
            Can be added later using 'addQuery' method.
            Columns:
            (strName : string
                Name for dataset. Cannot use '__', for file path.
            strSQL : string
                SQL statement as a string. Should be formatted for desired
                database.
            strDbType : {{0}}, optional
                Either strDbType or objCnxn must be provided to connect to
                database.
            objCnxn : pyodbc connection, optional
                If not provided, report will attempt to connect using string in
                config vile. Either strDbType or objCnxn must be provided to
                connect to database.
            dirDb : string, database location,optional
                 Must only be provided if strDbType is 'access' or 'mysql)
                 """.format([s for s in self.objCfg['ODBC']])
        def _defineVars(dfMetadata=dfMetadata):
            """For internal use. Helper function to adapt ReportTemplate.

            Parameters
            ----------
            dfMetadata : DataFrame, supplied by script
                Contains queries to be run during report."""
            # Base variables
            self.strMetadataFile = "Metadata.xlsx"
            self.dfMetadata = dfMetadata
            self.lstQueries = list(self.dfMetadata['strName'])

            # Class specific functions
            def _backupMetadata():
                """For internal use. Helper function to adapt backupData
                method to account for metadata file."""
                dirFile = os_path_join(self.dirBackup,
                                       "__" + self.strMetadataFile)
                self.log("Backing up metadata to '{0}'".format(dirFile),
                         'DEBUG')
                self.dfMetadata.to_excel(dirFile, index=False)
            self._backupMetadata = _backupMetadata

            def _delExcelFiles(dirFile):
                """For internal use. Helper function to adapt _delDataBackup
                method to account for metadata file."""
                if dirFile.split('.')[-1] == 'xlsx':
                    self.log("Removing '{0}'".format(dirFile))
                    os_remove(dirFile)
            self._delExcelFiles = _delExcelFiles

            def _restoreMetadata():
                """For internal use. Helper function to adapt _attemptResume
                method to account for metadata file."""
                dirFile = os_path_join(self.dirBackup, self.strMetadataFile)
                if os_path_isfile(dirFile):
                    self.log("Restoring metadata from '{0}'".format(dirFile))
                    self.dfMetadata = pd.read_excel(dirFile)
                    self.lstQueries = list(self.dfMetadata['strName'])
                    # Replace NaN with None
                    self.dfMetadata = self.dfMetadata.where(
                        pd.notnull(self.dfMetadata), None)
                    # Ensure names are strings
                    self.dfMetadata = self.dfMetadata.astype(
                        {'strName': 'str'})
                else:
                    self.log("Metadata not found! Restarting report.", 'ERROR')
                    self._delDataBackup()
            self._restoreMetadata = _restoreMetadata
        if dirLog is None and dirConfig is None:
            super().__init__(strName, funOptional=_defineVars)
        elif dirConfig is None:
            super().__init__(strName, dirLog, funOptional=_defineVars)
        elif dirLog is None:
            super().__init__(strName, dirConfig=dirConfig,
                             funOptional=_defineVars)
        else:
            super().__init__(strName, dirLog, dirConfig, _defineVars)

    def exportData(self, objFile, strReport, strSheet='', lstDirs=[]):
        """Export report data to report file. Will change file type to CSV as
        needed.

        Parameters
        ----------
        objFile : file object
            Must be parquet file. Contains report data for export.
        strReport : string
            Name of result file. File extension will be overwritten.
        strSheet: string, optional
            Name of excel sheet. Will append to file name if data is too large
            for Excel.
        lstDirs: list, optional
            All directories already used for export

        Returns
        -------
        Directories: list, final directory used for export appended to lstDirs
            key word arg"""
        lstDirs.append(super().exportData(objFile, strReport, strSheet))
        return lstDirs

    # @decLog
    def addQuery(self, strName, strSQL, strDbType=None,
                 objCnxn=None, dirDb=''):
        """Add query to list to be run upon execution. Alternative to setting
        dfMetadata upon initializing. Will reset .lstQueries after addition.

        Parameters
        ----------
        strName : string
            Name for dataset. Cannot use '__', for file path.
        strSQL : string
            SQL statement as a string. Should be formatted for desired database
        strDbType : {{0}}, optional
            Either strDbType or objCnxn must be provided to connect to
            database.
        objCnxn : pyodbc connection, optional
            If not provided, report will attempt to connect using string in
            config vile. Either strDbType or objCnxn must be provided to
            connect to database.
        dirDb : string, optional
             Database location. Must only be provided if strDbType is
             'access'.""".format([s for s in self.objCfg['ODBC']])
        if strName not in self.lstQueries:
            self.log("""Adding row to metadata with:
                         strName: {0}
                         strSQL: {1}
                         strDbType: {2}
                         objCnxn: {3}
                         dirDb: {4}""".format(strName, strSQL, strDbType,
                                              objCnxn, dirDb), 'DEBUG')
            self.dfMetadata = self.dfMetadata.append(pd.DataFrame(
                [[strName, strSQL, strDbType, objCnxn, dirDb]],
                columns=['strName', 'strSQL', 'strDbType', 'objCnxn',
                         'dirDb']), ignore_index=True)
            self.lstQueries = list(self.dfMetadata['strName'])
        else:
            self.log("Query with name '{0}' already exists. Query not added"
                     .format(strName), 'WARNING')

    # @decLog
    def removeQuery(self, strName):
        """Remove query from list to be run upon execution. See '.queries' for
        a list of all query names. Will reset .lstQueries after removal.

        Parameters
        ----------
        strName : string
            Name of query."""
        self.log("Removing query '{0}' from list".format(strName))
        self.dfMetadata = self.dfMetadata.drop(
            self.dfMetadata.loc[self.dfMetadata['strName'] == strName].index)
        self.lstQueries = list(self.dfMetadata['strName'])

    # @decLog
    def rename(self, strName):
        """Will rename report, changing file output.

        Parameters
        ----------
        strName: string
            Name of report"""
        self.log("Renaming report to '{0}'".format(strName))
        self.strName = strName
        self.objCfg['REPORT']['report_name'] = strName
        with open(self.dirCfg, 'w') as objFile:
            self.objCfg.write(objFile)

    # @decLog
    def reset(self):
        """Clear metadata, delete backup data, and reinitialize"""
        self.log("Reseting report")
        self._delDataBackup()
        self.__init__(self.strName, self.dirLog, self.dirCfg)

    # @decLog
    def run(self, bolMulti=True):
        """Will run all queries included in report then export to individual
        tabs named for each dataset. On CRITICAL failure, will backup data and
        attempt to resume report on next run.

        Parameters
        ----------
        bolMulti : boolean, default True
            Will utilize multithreading if True. Single thread is useful for
            debug.

        Returns
        -------
        Directories : list
            All unique directories where report was exported."""

        def _procQueries(lstDirs=[], bolMulti=True):
            """For internal use. Multithread section of run method.

            Parameters
            ----------
            lstDirs : list, optional
                Runs all queries in report. Utilizes single thread.

            Returns
            -------
            Directories : list
                All unique directories where report was exported."""
            # Loop metadata dataframe
            for _, row in self.dfMetadata.iterrows():
                # Schedule data retrieval
                dlyDataFile = dd(self.getData)(str(row['strName']),
                                               str(row['strSQL']),
                                               str(row['strDbType']),
                                               row['objCnxn'],
                                               str(row['dirDb']))[0]
                lstDirs = dd(self.exportData)(
                    dlyDataFile,
                    self.objCfg['REPORT']['export_to'],
                    str(row['strName']),
                    lstDirs)
            # Finalize dask schedule
            dlyEvent = dd(set)(lstDirs)
            # TODO: change ProgressBar implementation when tqdm supports dask
            with ProgressBar():
                if bolMulti:
                    self.log("Running with multithreading")
                    lstDirs = list(dlyEvent.compute())
                else:
                    self.log("Running on single thread")
                    lstDirs = list(dlyEvent.compute(
                        scheduler='single-threaded'))

            return lstDirs

        try:
            # Initialize directory list
            lstDirs = []
            # Check for queries
            if len(self.dfMetadata.index) > 0:
                lstDirs = _procQueries(lstDirs, bolMulti)
                # Rearrange sheets to match query order if needed
                if self._objWriter is not None:
                    dctOrder = dict((ws.title, ws) for ws in
                                    self._objWriter.book._sheets)
                    self._objWriter.book._sheets = [dctOrder.get(str(i)) for i
                                                    in self.lstQueries]
                    self._objWriter.save()
                # Inform user of export directories used
                self.log("Directory List:")
                for strDir in lstDirs:
                    self.log("'" + strDir + "'")
                self._delDataBackup()

                return lstDirs
            else:
                raise EmptyReport
        except Exception:
            self.log(tb_format_stack(), 'DEBUG')
            self.log("See log for debug details", 'CRITICAL')
            self.backupData()
            self.log("Backup successful")
            input("PRESS ANY KEY TO QUIT")
            self.log("QUITTING")
