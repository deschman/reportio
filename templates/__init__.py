# -*- coding: utf-8 -*-
"""Base template class for report objects."""


from abc import ABC, abstractmethod  # abstract (template) class
from sys import argv as sys_argv  # path finding
from os import listdir as os_listdir  # identify backup files
from os import makedirs as os_makedirs  # directory creation
from os import remove as os_remove  # delete backup files
from os.path import dirname as os_path_dirname  # root finding
from os.path import join as os_path_join  # path joinging
from os.path import isfile as os_path_isfile  # file validation
from os.path import isdir as os_path_isdir  # directory validation
from multiprocessing import cpu_count as mp_cpu_count  # for thread count
import configparser as cfg  # working with config file
from datetime import date as dt_date  # check backup files
from tempfile import NamedTemporaryFile  # send data to HDD to free up RAM
from sqlite3 import connect as sqlite3_connect  # connect to mysql

import pandas as pd  # assorted data wrangling and IO
import pyodbc  # connect to system databases
import dask.delayed as dd  # multithreading/scheduling
from openpyxl import load_workbook  # writing to multiple tabs

from ..logging import basicConfig as logging_basicConfig
from ..logging import log as logging_log
from ..errors import (ConfigError,
                      ReportNameError,
                      DBConnectionError,
                      DatasetNameError,
                      UnexpectedDbType)
from ..future.progress import ProgressBar
# TODO: implement decLog
# from ..logging import decLog as decLog


class ReportTemplate(ABC):

    # @decLog
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

    # @decLog
    def _delDataBackup(self, funOptional=None):
        """For internal use. Deletes all gz files in backup directory.

        Parameters
        ----------
        funOptional : function, optional
            Will be run for each file in backup dir with file directory as a
            parameter."""
        # Loop files in backup directory
        self.log("Cleaning up old data backup")
        for dirFile in [os_path_join(self.dirBackup, s) for s in
                        os_listdir(self.dirBackup)]:
            # Run optional function if included
            if callable(funOptional):
                funOptional(dirFile)
            if dirFile.split('.')[-1] in ['gz', 'txt']:
                self.log("Removing '{0}'".format(dirFile), 'DEBUG')
                os_remove(dirFile)

    # @decLog
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

    def __init__(self, strName, dirLog=os_path_join(os_path_dirname(
            sys_argv[0]), 'log.txt'), dirConfig=os_path_join(os_path_dirname(
                __file__), 'config.txt'), funOptional=None):
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
        strNewLog = logging_basicConfig(filename=self.dirLog)
        self.log = logging_log
        # Read config file if present
        self.objCfg = cfg.ConfigParser()
        if not os_path_isfile(self.dirCfg):
            self.log("Config location: '{0}'".format(self.dirCfg))
            raise ConfigError
        self.objCfg.read(self.dirCfg)
        self._dirTempFiles = self.objCfg['REPORT']['temp_files_folder']
        # Check if report name can be used
        try:
            NamedTemporaryFile(suffix='__' + self.strName).close()
        except OSError:
            raise ReportNameError(self.strName)
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

    # @decLog
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
                    raise DBConnectionError
                self.log("Unable to connect!", 'ERROR')
                self.log("Attempting login with UID/PWD from user.", 'DEBUG')
                strUID = input("Enter USERNAME: ")
                strPWD = input("Enter PASSWORD: ")
                strCnxn = 'UID=' + strUID + ';PWD=' + strPWD + ';' + strCnxn
        objCnxn = dctConnected.get(strDb)
        self.log("Using connection object '{0}'".format(objCnxn), 'DEBUG')
        return objCnxn

    # @decLog
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

    # @decLog
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
            raise DatasetNameError(strName)
        dfData.to_parquet(objFile, compression='gzip')
        self._lstFiles.append(objFile)
        return objFile

    # TODO: create a 'data' object for reporting module to reduce ambiguity
    # @decLog
    def getData(self, strName, strSQL, strDbType=None, objCnxn=None, dirDb='',
                ):
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
        Data : (file, DataFrame)
            Contains data from query.
        Connection : object
            fDatabase connection. From pyodbc module.""".format(
            [s for s in self.objCfg['ODBC']])
        # Check config file for connection string
        if strDbType not in self.objCfg['ODBC']:
            self.log("Config location: {0}".format(self.dirCfg), 'DEBUG')
            raise UnexpectedDbType(strDbType)
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
        if len(dfTemp.index) == 0:
            self.log("Query was empty", 'WARNING')
        if strName != '':
            objData = self.getTempFile(strName, dfTemp)
        else:
            objData = dfTemp
        return objData, objCnxn

    # @decLog
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
        varZero : user-defined type, default is pandas.NA
            Value to be used if query returns no results.

        Returns
        -------
        dfOutput : DataFrame
            dfInput with data populated in lstColumns from funSQL.""".format(
            [s for s in self.objCfg['ODBC']])
        def proc_chunk(dfInput, lstChunk, lstColumns=lstColumns,
                       objCnxn=objCnxn):
            # Initialize chunk dataframe
            dfChunk = pd.DataFrame(columns=lstColumns)
            # Loop chunk list
            for index, row in lstChunk:
                # Check for partial population
                if not any([pd.isna(dfInput.at[index, c])
                            for c in lstColumns]):
                    dfChunk.append(
                        pd.DataFrame([list(dfInput.loc[index])],
                                     index=[index],
                                     columns=dfChunk.columns))
                else:
                    dfQuery, objCnxn = self.getData('', funSQL(row),
                                                    strDbType, objCnxn)
                    try:
                        dfChunk.append(
                            pd.DataFrame([list(dfQuery.iat[0, 0])],
                                         index=[index],
                                         columns=dfChunk.columns))
                    # Handle 0 rows (no data found)
                    except IndexError:
                        dfChunk.append(
                            pd.DataFrame(
                                [[varZero] * len(lstColumns)],
                                index=[index],
                                columns=dfChunk.columns))
            return dfChunk
        # Add new columns if needed
        if any([c not in l for l in dfInput.columns for c in lstColumns]):
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
        for df in lstDfs:
            # Combine master df and chunk df, keeping value from latter
            dfOutput.combine(df,
                             lambda s1, s2: [v2 if not pd.isna(v2) else v1
                                             for v2 in s2 for v1 in s1],
                             overwrite=False)
        return dfOutput

    # @decLog
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
            if strHow == 'right':
                strSuffixR = None
                strSuffixL = "_drop"
            else:
                strSuffixR = '_drop'
                strSuffixL = None
            df = df.merge(df2, strHow, strMergeCol,
                          suffixes=(strSuffixL, strSuffixR))
            del df2
            df = df.drop([c for c in df if '_drop' in c], axis=1)
            if lstColsFin is not None:
                df = df[lstColsFin]
            df = df.drop_duplicates(ignore_index=True)
            objData = self.getTempFile(strName, df)
        return objData

    # @decLog
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
