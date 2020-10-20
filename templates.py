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
import configparser as cfg  # working with config file
from datetime import date as dt_date  # check backup files
from datetime import datetime as dt_dt  # timestamping
from tempfile import NamedTemporaryFile  # send data to HDD to free up RAM
import gzip  # file compression
from shutil import copyfileobj as shutil_copyfileobj  # copy data to gzip file
from gc import collect as gc_collect
import pandas as pd  # assorted data wrangling and IO
from fastparquet import ParquetFile  # working with oversided parquet files
import pyodbc  # connect to system databases
from sqlite3 import connect as sqlite3_connect  # connect to mysql
from openpyxl import load_workbook  # writing to multiple tabs
import dask.delayed as dd  # multithreading/scheduling
from mymodules.reporting.errors import LogError, ConfigError, ReportNameError,\
    ODBCConnectionError, UnexpectedDbType, DatasetNameError, EmptyReport

# Default variables
default_log_dir = os_path_join(os_path_dirname(sys_argv[0]), 'log.txt')
default_config_dir = os_path_join(os_path_dirname(__file__), 'config.txt')


# TODO: get this working as a decorator so that log file closes after use
def _decLog(fun=None):
    def wrapper(*args, **kwargs):
        # TODO: find 'self' object - untested
        report = kwargs.get('self')
        if report is None:
            for i in args:
                if hasattr(i, 'dirLog'):
                    report = i
        if report is None:
            report = globals().get('self')
        if report is None:
            raise LogError
        # Configure logger
        import logging
        strNewLog = "existing"
        try:
            if not os_path_isfile(report.dirLog):
                open(report.dirLog, 'w').close()
                strNewLog = "new"
            strFormat = '%(asctime)s %(levelname)s: %(message)s'
            logging.basicConfig(filename=report.dirLog, filemode='a',
                                format=strFormat, level=logging.DEBUG)
        except Exception as err:
            print(err)
            raise LogError
        if callable(fun):
            fun(args, kwargs)
        logging.shutdown()
        del logging
        gc_collect()
        return strNewLog
    return wrapper


class ReportTemplate(ABC):

    # @_decLog
    def log(self, strMsg, strLevel='INFO'):
        """Log feedback log file and to console as needed.

        Parameters
        ----------
        strMsg: string, message to log
        strLevel: string,
            {'DEBUG' only prints to log file;
            'INFO' prints to log file and console;
            'WARNING' prints to log file and console, may cause ERROR or
            CRITICAL issue;
            'ERROR' prints to log file and console, script attempts to handle;
            'CRITICAL' prints to log file and console, requires user input to
            close, script/config edits are needed to fix},
            default 'INFO'"""
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
        funOptional: function, will be run after files are copied but before
            this function is completed, default None"""
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
    def _attemptResume(self, funOptional=None):
        """For internal use. Checks backup folder for files that have been
        backed up today after a CRITICAL failure. Attempts to read files and
        resume report near the point of failure.

        Parameters
        ----------
        funOptional: function, will be run if data backup is found, default
            None

        Returns
        -------
        lstFiles: list, files found in backup"""
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
                if callable(funOptional):
                    funOptional()
            else:
                self.log("No recent backup files found", 'DEBUG')
        else:
            self.log("No backup found", 'DEBUG')
        return lstFiles

    # @_decLog
    def _delDataBackup(self, funOptional=None):
        """For internal use. Deletes all gz files in backup directory.

        Parameters
        ----------
        funOptional: function, will be run for each file in backup dir with
        file directory as a parameter, default None"""
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

    def __init__(self,
                 strName,
                 dirLog=default_log_dir,
                 dirConfig=default_config_dir,
                 funOptional=None):
        """Abstract template class for building custom reports. The 'run'
        method is not implemented.

        Parameters
        ----------
        strName: string, name of report
        dirLog: directory, file where script will log processes, will create
            new log file if directory does not exist, default is in user folder
        dirConfig: directory, config file, default is template config file
        funOptional: function, will run just before attempting resume, default
            None"""
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
            strFormat = '%(asctime)s %(levelname)s: %(message)s'
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
    def _read_parquet(self, objFile, lstCols=None):
        """OBSELETE. For internal use. Reads dataframe from parquet into
        pandas. Handles large files without throwing odd errors.

        Parameters
        ----------
        objFile: object, file containing data from pandas
        lstCols: list, columns to return from file, will return all if
            None, default None

        Returns
        -------
        DataFrame: object, contains data from file"""
        # TODO: follow up on bug report on github
        try:
            df = pd.read_parquet(objFile)
        # A data error is causing OSError, recover data and route through excel
        except OSError:
            # Ensure file is gzipped
            try:
                gzip.GzipFile(fileobj=objFile).read()
            except OSError:
                # Make a gzipped copy
                objFileGz = gzip.open(objFile.name + '.gz', 'wb')
                shutil_copyfileobj(objFile, objFileGz)
                objFile = objFileGz
            # Chunking prevents OSError but deletes tempfiles
            df = pd.concat((dfPart for dfPart in
                            ParquetFile(objFile).iter_row_groups()),
                           axis=0)
            # Create new tempfile with same data and name
            self.getTempFile(objFile.name.split('__')[-1], df)
            if 'objFileGz' in locals():
                objFileGz.close()
                os_remove(objFileGz)
        if lstCols is not None:
            df = df[lstCols]
        return df

    # @_decLog
    def _getConnection(self, strDb, strCnxn, dctConnected={}):
        """For internal use. Creates pyodbc connection object using connection
        string from config file. Will prompt user for UID/PWD as needed.

        Parameters
        ----------
        strDb: string, database name for logging messages
        strCnxn: string, must be formatted for database
        dctConnected: list, default creates new empty list

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
        """For internal use.

        Parameters
        ----------
        dirReport: directory, location of result report

        Returns
        -------
        Writer: object, Excel writer from pandas module utilizing openpyxl
        as engine"""
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
        strName: string, name appended to end of temporary file
        dfData: DataFrame, data to be written to temporary file
        dirFolder: directory, folder where file is stored, will default to
            config location if None, default None

        Returns
        -------
        File: object, from tempfile module, contains data from dataframe
            parameter"""
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

    # @_decLog
    def getData(self, strTempFile, strSQL, strDbType, objCnxn=None, dirDb=''):
        """Retrieve data from database via odbc. Will prompt user for login as
        needed.

        Parameters
        ----------
        strTempFile: string, name for dataset, cannot use '__' for file path,
            leave as empty string to return dataframe instead of temporary file
        strSQL: string, SQL statement, formatted for desired database
        strDbType: {'ozark1', 'datawhse', 'sailfish', 'access', 'mysql'}
        objCnxn: pyodbc connection object, default None
        dirDb: string, database location, must be provided if strDbType is
            'access', default None

        Returns
        -------
        Data: object, contains data from query
        Connection: object, from pyodbc module, database connection"""
        # Check config file for connection string
        if strDbType not in self.objCfg['ODBC']:
            self.log("Config location: {0}".format(self.dirCfg), 'DEBUG')
            raise UnexpectedDbType(self.log, strDbType)
        dirBackupFile = os_path_join(self.dirBackup, strTempFile + '.gz')
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
        if strTempFile != '':
            objData = self.getTempFile(strTempFile, dfTemp)
        else:
            objData = dfTemp
        return objData, objCnxn

    # @_decLog
    def mergeFiles(self, strTempFile, objFile1, objFile2, strMergeCol,
                   strHow='inner', lstCols1=None, lstCols2=None,
                   lstColsFin=None):
        """Merges two parquet files into one

        Parameters
        ----------
        strTempFile : string, output file name
        objFile1: object, file containing data from pandas
        objFile2: object, file containing data from pandas
        strMergeCol: string, column used for merge
        strHow: string, merge type
        lstCols1: list, columns merged from first file, default all columns
        lstCols2: list, columns merged from second file, default all columns
        lstColsFin: list, columns listed in result file

        Returns
        -------
        objData : object, merged file"""
        # Check for backup file
        dirBackupFile = os_path_join(self.dirBackup, strTempFile + '.gz')
        if os_path_isfile(dirBackupFile):
            self.log("Reading backup file")
            dfTemp = pd.read_parquet(dirBackupFile)
            if strTempFile != '':
                objData = self.getTempFile(strTempFile, dfTemp)
        else:
            self.log("Merging files")
            df = pd.read_parquet(objFile1, columns=lstCols1)
            df2 = pd.read_parquet(objFile2, columns=lstCols2)
            df = df.merge(df2, strHow, strMergeCol)
            del df2
            if lstColsFin is not None:
                df = df[lstColsFin]
            df = df.drop_duplicates(ignore_index=True)
            objData = self.getTempFile(strTempFile, df)
        return objData

    # @_decLog
    def exportData(self, objFile, dirReport, strSheet='', objXlWriter=None):
        """Export report data to report file. Will change file type to CSV as
        needed.

        Parameters
        ----------
        objFile: file object, parquet file, report data for output
        dirReport: directory, location of result file, should not include file
            extension
        strSheet: string, name of excel sheet, will append to file name if data
            too large for Excel, default None

        Returns
        -------
        Directory: directory, final directory used for export"""
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
        lstFiles: list, files found in backup"""
        super()._attemptResume(self._restoreMetadata)

    def __init__(self,
                 strName,
                 dirLog=default_log_dir,
                 dirConfig=default_config_dir,
                 dfMetadata=pd.DataFrame(
                     columns=['strName', 'strSQL', 'strDbType', 'objCnxn',
                              'dirDb'])):
        """Will connect, query, and export data for each row in dfMetadata.
        Metadata may be added using .addQuery after creation of the
        SimpleReport object.

        Parameters
        ----------
        strName: string, name of report
        dirLog: directory, file where script will log processes, will create
            new log file if directory does not exist, default None
        dirConfig: directory, config file, default is template config file.
        dfMetadata: optional, can be added later using 'addQuery' method,
            DataFrame object, from pandas module, columns:
            (strName: string, name for dataset, cannot use '__', for file path
             strSQL: string, SQL statement, formatted for desired database
             strDbType: {'ozark1', 'datawhse', 'sailfish', 'access', 'mysql'}
             objCnxn: pyodbc connection object, default None
             dirDb: string, database location, must be provided if strDbType is
             'access' or 'mysql', default None)"""
        def _defineVars(dfMetadata=dfMetadata):
            # Base variables
            self.strMetadataFile = "Metadata.xlsx"
            self.dfMetadata = dfMetadata
            self.lstQueries = list(self.dfMetadata['strName'])

            # Class specific functions
            def _backupMetadata():
                dirFile = os_path_join(self.dirBackup,
                                       "__" + self.strMetadataFile)
                self.log("Backing up metadata to '{0}'".format(dirFile),
                         'DEBUG')
                self.dfMetadata.to_excel(dirFile, index=False)
            self._backupMetadata = _backupMetadata

            def _delExcelFiles(dirFile):
                if dirFile.split('.')[-1] == 'xlsx':
                    self.log("Removing '{0}'".format(dirFile))
                    os_remove(dirFile)
            self._delExcelFiles = _delExcelFiles

            def _restoreMetadata():
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

    def exportData(self, objFile, dirReport, strSheet='', lstDirs=[]):
        """Export report data to report file. Will change file type to CSV as
        needed.

        Parameters
        ----------
        objFile: file object, parquet file, report data for output
        dirReport: directory, location of result file, should not include file
            extension
        strSheet: string, name of excel sheet, will append to file name if data
            too large for Excel, default None
        lstDirs: list, all directories used

        Returns
        -------
        Directories: list, final directory used for export appended to lstDirs
            key word arg"""
        lstDirs.append(super().exportData(objFile, dirReport, strSheet))
        return lstDirs

    # @_decLog
    def addQuery(self, strName, strSQL, strDbType, objCnxn=None, dirDb=''):
        """Add query to list to be run upon execution. Alternative to setting
        dfMetadata upon initializing. Will reset .lstQueries after addition.

        Parameters
        ----------
        strName: string, name for dataset, cannot use '__', for file path
        strSQL: string, SQL statement, formatted for desired database
        strDbType: {'ozark1', 'datawhse', 'sailfish', 'access', 'mysql'}
        objCnxn: pyodbc connection object, default None
        dirDb: string, database location, must be provided if strDbType is
            'access' or 'mysql', default None"""
        if strName not in self.lstQueries:
            self.log("""Adding row to metadata with:
                         strName: {0}
                         strSQL: {1}
                         strDbType: {2}
                         objCnxn: {3}
                         dirDb: {4}""".format(strName, strSQL, strDbType,
                                              objCnxn is None, dirDb), 'DEBUG')
            self.dfMetadata = self.dfMetadata.append(pd.DataFrame(
                [[strName, strSQL, strDbType, objCnxn, dirDb]],
                columns=['strName', 'strSQL', 'strDbType', 'objCnxn', 'dirDb']
                ), ignore_index=True)
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
        strName: string, name for dataset of dataset query"""
        self.log("Removing query '{0}' from list".format(strName))
        self.dfMetadata = self.dfMetadata.drop(
            self.dfMetadata.loc[self.dfMetadata['strName'] == strName].index)
        self.lstQueries = list(self.dfMetadata['strName'])

    # @_decLog
    def rename(self, strName):
        """Will rename report, changing file output.

        Parameters
        ----------
        strName: string, name of report"""
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
        bolMulti: boolean, will utilize multithreading if True, single thread
            useful for debug, default True

        Returns
        -------
        Directories: list, all unique directories where report was exported"""

        # Define single thread function
        def singleThread(lstDirs=[]):
            # Loop metadata dataframe
            for _, row in self.dfMetadata.iterrows():
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
        def multiThread(lstDirs=[]):
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
            lstDirs = list(dlyEvent.compute())

            return lstDirs

        try:
            # Initialize directory list
            lstDirs = []
            # Check for queries
            if len(self.dfMetadata.index) > 0:
                if bolMulti:
                    self.log("Running with multithreading")
                    lstDirs = multiThread()
                else:
                    self.log("Running on single thread")
                    lstDirs = singleThread()
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
            self._backupData()
            self.log("Backup successful")
            input("PRESS ANY KEY TO QUIT")
            self.log("QUITTING")
