# -*- coding: utf-8 -*-


from abc import ABC, abstractmethod  # abstract (template) class
from sys import argv as sys_argv  # path finding
from os import listdir as os_listdir  # identify backup files
from os import makedirs as os_makedirs  # directory creation
from os import remove as os_remove  # delete backup files
from os.path import dirname as os_path_dirname  # root finding
from os.path import join as os_path_join  # path joinging
from os.path import isfile as os_path_isfile  # file validation
from os.path import isdir as os_path_isdir  # directory validation
import logging  # process logging
import threading  # thread identification
from multiprocessing import cpu_count as mp_cpu_count  # for thread count
import configparser as cfg  # working with config file
from datetime import date as dt_date  # check backup files
from datetime import datetime as dt_dt  # timestamping
from tempfile import NamedTemporaryFile  # send data to HDD to free up RAM
import pandas as pd  # assorted data wrangling and IO
import pyodbc  # connect to system databases
import dask.delayed as dd  # multithreading/scheduling
from sqlite3 import connect as sqlite3_connect  # connect to mysql
from openpyxl import load_workbook  # writing to multiple tabs
from tqdm import tqdm  # progress bar
from reporting.errors import LogError, ConfigError, ReportNameError,\
    ODBCConnectionError, UnexpectedDbType, DatasetNameError, EmptyReport
# TODO: implement _decLog
# from reporting.decorators import decLog as _decLog
from reporting.future.progress import ProgressBar

# Default variables
default_log_dir = os_path_join(os_path_dirname(sys_argv[0]), 'log.txt')
default_config_dir = os_path_join(os_path_dirname(__file__), 'config.txt')


class ReportTemplate(ABC):

    # @_decLog
    def log(self, strMsg, strLevel='INFO'):
        """Log feedback log file and to console as needed.

        Parameters
        ----------
        strMsg : string
            Message to be logged.
        strLevel: {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
            default 'INFO'
            'DEBUG' : Only prints to log file.
            'INFO' : Prints to log file and console.
            'WARNING :' Prints to log file and console. May cause ERROR or
                CRITICAL issue.
            'ERROR :' Prints to log file and console. Script attempts to
                handle.
            'CRITICAL' : Prints to log file and console. Requires user input to
                close. Script/config edits are needed to fix."""
        # Prevents spam by redirecting log level to debug when multithreading
        if threading.current_thread() is not threading.main_thread():
            strLevel = "DEBUG"
        if strLevel == 'DEBUG':
            logging.debug(strMsg)
        elif strLevel == 'INFO':
            logging.info(strMsg)
            print(dt_dt.now(), strLevel + ':', strMsg)
        elif strLevel == 'WARNING':
            logging.warning(strMsg)
            print(dt_dt.now(), strLevel + ':', strMsg)
        elif strLevel == 'ERROR':
            logging.error(strMsg)
            print(dt_dt.now(), strLevel + ':', strMsg)
        elif strLevel == 'CRITICAL':
            logging.critical(strMsg)
            print(dt_dt.now(), strLevel + ':', strMsg)

    # @_decLog
    def backupData(self, funOptional=None):
        """Saves temporary files to backup folder in the event of CRICITAL
        failure. Backup files are utilized if script is run again the same day
        as the backup.

        Parameters
        ----------
        funOptional : function, optional
            Will be run after files are copied but before this function is
            completed."""
        self.log("Backing up data", 'WARNING')
        self.log("Backup location: {0}".format(self.dirBackup), 'DEBUG')
        # Copy parquet files
        for objFile in self._lstFiles:
            if hasattr(objFile, 'name'):
                if objFile.name.split('.')[-1] == 'gz' and os_path_isfile(
                        objFile.name):
                    try:
                        dirDst = os_path_join(self.dirBackup,
                                              objFile.name.split('__')[-1])
                        self.log("Backing up '{0}' to '{1}'".format(
                            objFile.name, objFile.name), 'DEBUG')
                        pd.read_parquet(objFile).to_parquet(dirDst,
                                                            compression='gzip')
                    # If one backup fails, try other files
                    except AttributeError:
                        continue
        # Run optional function if included
        if callable(funOptional):
            funOptional()
        # Write text file with start date
        with open(os_path_join(self.dirBackup, 'startDate.txt'),
                  'w') as objFile:
            objFile.write(self.startDate)

    # @_decLog
    def _delDataBackup(self, funOptional=None):
        """For internal use. Deletes all gz files in backup directory.

        Parameters
        ----------
        funOptional : function, optional
            Will be run for each file in backup dir with file directory as a
            parameter."""
        # Loop files in backup directory
        self.log("Cleaning up")
        for dirFile in [os_path_join(self.dirBackup, s) for s in
                        os_listdir(self.dirBackup)]:
            # Run optional function if included
            if callable(funOptional):
                funOptional(dirFile)
            if dirFile.split('.')[-1] in ['gz', 'txt']:
                self.log("Removing '{0}'".format(dirFile), 'DEBUG')
                os_remove(dirFile)

    # @_decLog
    def _attemptResume(self, funOptional1=None, funOptional2=None):
        """For internal use. Checks backup folder for files that have been
        backed up today after a CRITICAL failure. Attempts to read files and
        resume report near the point of failure.

        Parameters
        ----------
        funOptional1 : function, optional
            Will be run if data backup is found.
        funOptional2 : function, optional
            Will be run if data backup is not from today as funOptional
            parameter in self._delDataBackup.

        Returns
        -------
        lstFiles : list
            Files found in backup."""
        self.log("Checking for backup files", 'DEBUG')
        lstFiles = []
        # Look for date stamp file
        strFile = 'startDate.txt'
        if strFile in os_listdir(self.dirBackup):
            with open(os_path_join(self.dirBackup, strFile), 'r') as objFile:
                strStartDate = objFile.read()
            # Check if date stamp matches today
            if strStartDate == dt_date.today().isoformat():
                self.log("Resuming previous attempt")
                # Gather names of files to read from backup
                for strFile in [f for f in os_listdir(self.dirBackup)
                                if f.split('.')[-1] == 'gz']:
                    lstFiles.append(strFile)
                self.log("These files will be read from backup: {0}"
                         .format(lstFiles), 'DEBUG')
                # Run optional function if included
                if callable(funOptional1):
                    funOptional1()
            else:
                self.log("No recent backup files found", 'DEBUG')
                if callable(funOptional2):
                    self._delDataBackup(funOptional2)
                else:
                    self._delDataBackup()
        else:
            self.log("No backup found", 'DEBUG')
        return lstFiles

    def __init__(self, strName, dirLog=default_log_dir,
                 dirConfig=default_config_dir, funOptional=None):
        """Abstract template class for building custom reports. The 'run'
        method is not implemented.

        Parameters
        ----------
        strName : string
            Name of report.
        dirLog : directory, default is in user folder
            File where script will log processes. Will create new log file if
            directory does not exist.
        dirConfig:  directory, default is template config file
            Location of config file.
        funOptional : function, optional
            Will run just before attempting resume."""
        # Assign base variables
        self.strName = strName
        self.dirCfg = dirConfig
        self.dirLog = dirLog
        self.startDate = dt_date.today().isoformat()
        self._intSheets = 1
        self._lstFiles = []
        # Prevent odbc pooling, which causes errors
        if pyodbc.pooling:
            pyodbc.pooling = False
        # Configure logger
        # _decLog.wrapper(self)
        strNewLog = "existing"
        try:
            if not os_path_isfile(self.dirLog):
                open(self.dirLog, 'w').close()
                strNewLog = "new"
            strFormat = '%(asctime)s %(threadName)s %(levelname)s: %(message)s'
            logging.basicConfig(filename=self.dirLog, filemode='a',
                                format=strFormat, level=logging.DEBUG)
        except Exception as err:
            print(err)
            raise LogError
        # Read config file if present
        self.objCfg = cfg.ConfigParser()
        if not os_path_isfile(self.dirCfg):
            self.log("Config location: '{0}'".format(self.dirCfg))
            raise ConfigError(self.log)
        self.objCfg.read(self.dirCfg)
        self._dirTempFiles = self.objCfg['REPORT']['temp_files_folder']
        # Check if report name can be used
        try:
            NamedTemporaryFile(suffix='__' + self.strName).close()
        except OSError:
            raise ReportNameError(self.log, self.strName)
        # Write unpopulated variables
        if sys_argv[0] == '':
            self.selfDir = os_path_dirname(__file__)
            self.objCfg['REPORT']['self_dir'] = self.selfDir
            self.objCfg['REPORT']['self_folder'] = os_path_dirname(
                self.selfDir)
        else:
            self.selfDir = os_path_dirname(sys_argv[0])
            self.objCfg['REPORT']['self_dir'] = self.selfDir
            self.objCfg['REPORT']['self_folder'] = os_path_dirname(
                self.selfDir)
        self.objCfg['REPORT']['report_name'] = self.strName
        with open(self.dirCfg, 'w') as objFile:
            self.objCfg.write(objFile)
        # Make backup dir if needed
        self.dirBackup = self.objCfg['REPORT']['backup_folder']
        if not os_path_isdir(self.dirBackup):
            self.log("Creating backup dir at '{0}'".format(self.dirBackup),
                     'DEBUG')
            os_makedirs(self.dirBackup)
        # Make temporary files dir if needed
        self.dirTempFolder = self.objCfg['REPORT']['temp_files_folder']
        if not os_path_isdir(self.dirTempFolder):
            self.log("Creating temp dir at '{0}'".format(
                self.dirTempFolder))
            os_makedirs(self.dirTempFolder)
        # Inform user
        self.log("Starting report with {0} log located at '{1}'"
                 .format(strNewLog, dirLog))
        self.log("Variables: selfDir: '{0}', dirLog: '{1}', dirConfig: '{2}'"
                 .format(self.selfDir, dirLog, dirConfig), 'DEBUG')
        # Run optional function if callable
        if callable(funOptional):
            funOptional()
        # Attempt resume
        self.lstFiles = self._attemptResume()

    # @_decLog
    def _getConnection(self, strDb, strCnxn, dctConnected={}):
        """For internal use. Creates pyodbc connection object using connection
        string from config file. Will prompt user for UID/PWD as needed.

        Parameters
        ----------
        strDb : string
            Database name.
        strCnxn : string
            Connection string. Must be formatted for database.
        dctConnected : dictionary, optional

        Returns
        -------
        Connection: object, from pyodbc module
        Connection Dictionary: dictionary, dctConnected parameter plus any new
            connection created"""
        # Initialize login iteration
        i = 0
        # Loop until connected
        while strDb not in dctConnected.keys():
            try:
                self.log("Connecting to {0}".format(strDb))
                if strDb in ['ozark1', 'datawhse']:
                    objCnxn = pyodbc.connect(strCnxn)
                elif strDb == 'mysql':
                    objCnxn = sqlite3_connect(strCnxn)
                dctConnected[strDb] = objCnxn
                self.log("Connection successful", 'DEBUG')
            # If login fails, get user id/password and retry
            except pyodbc.Error as err:
                i += 1
                # Strip previous user entry if present
                strCnxn = 'DSN' + strCnxn.split('DSN')[-1]
                # Cap login attempts at 5
                if i > 5:
                    del strUID
                    del strPWD
                    self.log(err, 'DEBUG')
                    self.log("Connection string (without UID/PWD): {0}"
                             .format(strCnxn), 'DEBUG')
                    raise ODBCConnectionError(self.log)
                self.log("Unable to connect!", 'ERROR')
                self.log("Attempting login with UID/PWD from user.", 'DEBUG')
                strUID = input("Enter USERNAME: ")
                strPWD = input("Enter PASSWORD: ")
                strCnxn = 'UID=' + strUID + ';PWD=' + strPWD + ';' + strCnxn
        objCnxn = dctConnected.get(strDb)
        self.log("Using connection object '{0}'".format(objCnxn), 'DEBUG')
        return objCnxn

    # @_decLog
    def _getWriter(self, dirReport):
        """For internal use. Creates ExcelWriter object to allow multiple tabs
        to be written.

        Parameters
        ----------
        dirReport : directory
            Location of result report.

        Returns
        -------
        ExcelWriter : object
            From pandas module utilizing openpyxl as engine."""
        # Creates blank Excel file if needed
        if not os_path_isfile(dirReport):
            pd.DataFrame().to_excel(dirReport)
        # Creates Excel Writer
        objWriter = pd.ExcelWriter(dirReport, engine='openpyxl')
        objWriter.book = load_workbook(dirReport)
        objWriter.sheets = dict((ws.title, ws) for ws in
                                objWriter.book.worksheets)
        return objWriter

    # @_decLog
    def getTempFile(self, strName, dfData, dirFolder=None):
        """Creates tempfile object and writes dataframe to
        it. File will always use gzip compression and end in .gz. Raises
        DatasetNameError if strName cannot be used in file. Overwriting may
        cause a critical error and data corruption.

        Parameters
        ----------
        strName : string
            Name appended to end of temporary file.
        dfData : DataFrame
            Data to be written to temporary file.
        dirFolder : directory, default is location in config
            Folder where file is stored.

        Returns
        -------
        File : tempfile
            Contains data from dfData. From tempfile module."""
        if dirFolder is None:
            dirFolder = self.dirTempFolder
        try:
            objFile = NamedTemporaryFile(dir=dirFolder,
                                         suffix="__" + strName + '.gz')
        except OSError as err:
            self.log(err, 'DEBUG')
            raise DatasetNameError(self.log, strName)
        dfData.to_parquet(objFile, compression='gzip')
        self._lstFiles.append(objFile)
        return objFile

    # TODO: creat a 'data' object for reporting module to reduce ambiguity
    # @_decLog
    def getData(self, strName, strSQL, strDbType=None, objCnxn=None, dirDb=''):
        """Retrieve data from database via odbc. Will prompt user for login as
        needed.

        Parameters
        ----------
        strName : string
            Name for dataset. Cannot use '__', for file path.
        strSQL : string
            SQL statement as a string. Should be formatted for desired database
        strDbType : {{0}}, optional
            Either strDbType or objCnxn must be provided to connect to
            database.
        objCnxn : connection, optional
            If not provided, report will attempt to connect using string in
            config vile. Either strDbType or objCnxn must be provided to
            connect to database. From pyodbc module.
        dirDb : string, optional
             Database location. Must only be provided if strDbType is
             'access'.

        Returns
        -------
        Data : {file, DataFrame}
            Contains data from query.
        Connection : object
            fDatabase connection. From pyodbc module.""".format(
            [s for s in self.objCfg['ODBC']])
        # Check config file for connection string
        if strDbType not in self.objCfg['ODBC']:
            self.log("Config location: {0}".format(self.dirCfg), 'DEBUG')
            raise UnexpectedDbType(self.log, strDbType)
        dirBackupFile = os_path_join(self.dirBackup, strName + '.gz')
        if not os_path_isfile(dirBackupFile):
            # Create new connection if needed
            if objCnxn is None:
                objCnxn = self._getConnection(strDbType,
                                              self.objCfg['ODBC'][strDbType])
            self.log("Querying database")
            dfTemp = pd.read_sql(strSQL, objCnxn)
        else:
            self.log("Reading backup file")
            dfTemp = pd.read_parquet(dirBackupFile)
        if strName != '':
            objData = self.getTempFile(strName, dfTemp)
        else:
            objData = dfTemp
        return objData, objCnxn

    # @_decLog
    def crossQuery(self, dfInput, lstColumns, funSQL, strDbType=None,
                   objCnxn=None, dirDb='', varZero=pd.NA):
        """For experimental use. Append lstColumns to dfInput from data from
        another query. Wil dynamically build queries based on values in each
        row of dfInput and execute while utilizing dask for multithread
        scheduling.

        Parameters
        ----------
        dfInput : DataFrame
            DataFrame to be appended with data from queries.
        lstColumns : list
            Name of new columns to be added.
        funSQL : function
            Should have input for row of data from DataFrame.iterrows() and
            return query as string.
        strDbType : {{0}}, optional
            Either strDbType or objCnxn must be provided to connect to
            database.
        objCnxn : pyodbc connection, optional
            If not provided, report will attempt to connect using string in
            config vile. Either strDbType or objCnxn must be provided to
            connect to database.
        dirDb : string, optional
             Database location. Must only be provided if strDbType is
             'access'.
        varZero : user-defined type, optional
            Value to be used if query returns no results. The default is
            pandas.NA.

        Returns
        -------
        dfOutput : DataFrame
            dfInput with data populated in lstColumns from funSQL.""".format(
            [s for s in self.objCfg['ODBC']])
        def proc_chunk(dfInput, lstChunk, lstColumns=lstColumns):
            # Initialize chunk dataframe
            dfChunk = pd.DataFrame(columns=['INDEX'] + lstColumns)
            # Loop chunk list
            for index, row in lstChunk:
                # Check for partial population
                if not any([pd.isna(dfInput.at[index, c])
                            for c in lstColumns]):
                    dfChunk.append(
                        pd.DataFrame([[index] + list(dfInput.loc[index])],
                                     columns=dfChunk.columns))
                else:
                    dfQuery, objCnxn = self.getData('', funSQL(row),
                                                    strDbType, objCnxn)
                    try:
                        dfChunk.append(
                            pd.DataFrame([[index] + list(dfQuery.iat[0, 0])],
                                         columns=dfChunk.columns))
                    # Handle 0 rows (no data found)
                    except IndexError:
                        dfChunk.append(
                            pd.DataFrame(
                                [[index] + [varZero] * len(lstColumns)],
                                columns=dfChunk.columns))
            return dfChunk
        # Add new columns if needed
        for c in lstColumns:
            if c not in dfInput.columns:
                dfInput[c] = pd.NA
        # Initialize chunk list, dataframe list
        lstChunk = []
        lstDfs = []
        # Loop dataframe
        for index_row in dfInput.iterrows():
            lstChunk.append(index_row)
            # Process in chunks equal to number of threads available
            if len(lstChunk) >= len(dfInput.index) / mp_cpu_count():
                lstDfs.append(dd(proc_chunk)(dfInput, lstChunk))
            lstChunk = []
        # Attempt connection before spamming with all threads
        self._getConnection(strDbType, self.objCfg['ODBC'][strDbType])
        with ProgressBar():
            dd(len)(lstDfs).compute()
        # Initialize output dataframe
        dfOutput = dfInput.copy()
        for c in lstColumns:
            dfOutput.drop(c)
        for df in lstDfs:
            dfOutput.merge(df, how='left', left_index=True, right_on='INDEX'
                           ).drop('INDEX')
        return dfOutput

    # @_decLog
    def mergeFiles(self, strName, objFile1, objFile2, strMergeCol,
                   strHow='inner', lstCols1=None, lstCols2=None,
                   lstColsFin=None):
        """Merges two parquet files into one

        Parameters
        ----------
        strName : string
            Output file name.
        objFile1 : file
            Contains data from pandas.
        objFile2 : file
            Contains data from pandas.
        strMergeCol : string
            Column used for merge.
        strHow : {'left', 'right', 'outer', 'inner'}, default 'inner'
            Merge type.
        lstCols1 : list, default all columns
            Columns merged from first file.
        lstCols2 : list, default all columns
            Columns merged from second file.
        lstColsFin : list, default all columns
            Columns listed in result file.

        Returns
        -------
        objData : object, merged file"""
        # Check for backup file
        dirBackupFile = os_path_join(self.dirBackup, strName + '.gz')
        if os_path_isfile(dirBackupFile):
            self.log("Reading backup file")
            dfTemp = pd.read_parquet(dirBackupFile)
            if strName != '':
                objData = self.getTempFile(strName, dfTemp)
        else:
            self.log("Merging files")
            df = pd.read_parquet(objFile1, columns=lstCols1)
            df2 = pd.read_parquet(objFile2, columns=lstCols2)
            df = df.merge(df2, strHow, strMergeCol)
            del df2
            if lstColsFin is not None:
                df = df[lstColsFin]
            df = df.drop_duplicates(ignore_index=True)
            objData = self.getTempFile(strName, df)
        return objData

    # @_decLog
    def exportData(self, objFile, dirReport, strSheet='', objXlWriter=None):
        """Export report data to report file. Will change file type to CSV as
        needed.

        Parameters
        ----------
        objFile : file
            Parquet file. Report data for output.
        dirReport : directory
            Location of result file. File extension will be overwritten
        strSheet : string, optional
            Name of excel sheet. Will append to file name if data too large for
            Excel.
        objXlWriter : ExcelWriter, optional
            For use if writing multiple tabs. From pandas module.

        Returns
        -------
        Directory : directory
            Final directory used for export."""
        # Check file format
        if len(dirReport.split('.')) > 1:
            self.log("File extension will be overwritten",
                     'WARNING')
            self.log("Attempted report location: '{0}'".format(dirReport),
                     'DEBUG')
            dirReport = dirReport.split('.')[0]
        # Read data file
        dfData = pd.read_parquet(objFile)
        # Check file size
        if len(dfData.index) > 1048576 or len(dfData.columns) > 16384:
            self.log("Exporting data to CSV")
            dirReport = dirReport + "__" + strSheet + '.csv'
            dfData.to_csv(dirReport, index=False)
        else:
            self.log("Exporting data to Excel")
            # Handle user variables
            if strSheet == '':
                strSheet = 'Sheet' + str(self._intSheets)
            dirReport = dirReport + '.xlsx'
            # Write to Excel
            if objXlWriter is None:
                self._objWriter = self._getWriter(dirReport)
            else:
                self._objWriter = objXlWriter
            dfData.to_excel(self._objWriter, strSheet, index=False)
            self._objWriter.save()
        self._intSheets += 1
        return dirReport

    @abstractmethod
    def run(self):
        pass


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

    def __init__(self, strName, dirLog=default_log_dir,
                 dirConfig=default_config_dir, dfMetadata=pd.DataFrame(
                     columns=['strName', 'strSQL', 'strDbType', 'objCnxn',
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

    # @_decLog
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

    # @_decLog
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

    # @_decLog
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

    # @_decLog
    def reset(self):
        """Clear metadata, delete backup data, and reinitialize"""
        self.log("Reseting report")
        self._delDataBackup()
        self.__init__(self.strName, self.dirLog, self.dirCfg)

    # @_decLog
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

        # Define single thread function
        def _singleThread(lstDirs=[]):
            """For internal use. Single thread section of run method.

            Parameters
            ----------
            lstDirs : list, optional
                Runs all queries in report. Utilizes single thread.

            Returns
            -------
            Directories : list
                All unique directories where report was exported."""
            # Loop metadata dataframe
            for _, row in tqdm(self.dfMetadata.iterrows()):
                # Get data
                objDataFile = self.getData(str(row['strName']),
                                           str(row['strSQL']),
                                           str(row['strDbType']),
                                           row['objCnxn'],
                                           str(row['dirDb']))[0]
                # Export data
                lstDirs = self.exportData(objDataFile,
                                          self.objCfg['REPORT']['export_to'],
                                          str(row['strName']),
                                          lstDirs)
            return list(set(lstDirs))

        # Define multiple thread function
        def _multiThread(lstDirs=[]):
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
            # TODO: change this implementation when tqdm supports dask
            with ProgressBar():
                lstDirs = list(dlyEvent.compute())

            return lstDirs

        try:
            # Initialize directory list
            lstDirs = []
            # Check for queries
            if len(self.dfMetadata.index) > 0:
                if bolMulti:
                    self.log("Running with multithreading")
                    lstDirs = _multiThread()
                else:
                    self.log("Running on single thread")
                    lstDirs = _singleThread()
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
                raise EmptyReport(self.log)
        except Exception as err:
            self.log(err, 'DEBUG')
            self.log("See log for debug details", 'CRITICAL')
            self.backupData()
            self.log("Backup successful")
            input("PRESS ANY KEY TO QUIT")
            self.log("QUITTING")
