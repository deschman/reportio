# -*- coding: utf-8 -*-


class ReportError(Exception):
    def __init__(self, strMsg, objLog):
        """Base exception class for reporting module. Will call log object if
        it is a function, otherwise will print message and try to call
        .critical. Will always return message.

        Parameters
        ----------
        strMsg: string, error message
        objLog: function or logging object

        Returns
        -------
        message: string, pass through from error call"""
        # Check if logging object is function
        if callable(objLog):
            objLog(strMsg, 'CRITICAL')
        else:
            print(strMsg)
            objLog.critical(strMsg)
        super().__init__()
        return strMsg


class LogError(Exception):
    def __init__(self):
        """Error encountered while configuring log for report."""
        strMsg = "unable to configure log"
        print(strMsg)
        super().__init__()
        return strMsg


class ConfigError(ReportError):
    def __init__(self, objLog):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object

        Returns
        -------
        message: string, from error call"""
        super().__init__("config file not found", objLog)


class ReportNameError(ReportError):
    def __init__(self, objLog, strName):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object
        strName: string, report name attempted to use

        Returns
        -------
        message: string, from error call"""
        super().__init__("cannot use '{0}' as report name".format(strName),
                         objLog)


class ODBCConnectionError(ReportError):
    def __init__(self, objLog):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object

        Returns
        -------
        message: string, from error call"""
        super().__init__("connection to database failed", objLog)


class UnexpectedDbType(ReportError):
    def __init__(self, objLog, strType):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object
        strType: string, database type attempted to connect to

        Returns
        -------
        message: string, from error call"""
        super().__init__("database type '{0}' not excpected".format(strType),
                         objLog)


class DatasetNameError(ReportError):
    def __init__(self, objLog, strName):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object
        strName: string, dataset name attempted to use

        Returns
        -------
        message: string, from error call"""
        super().__init__("cannot use '{0}' as dataset name", objLog)


class EmptyReport(ReportError):
    def __init__(self, objLog):
        """See ReportError for base class info.

        Parameters
        ----------
        objLog: function or logging object

        Returns
        -------
        message: string, from error call"""
        super().__init__("report contains no queries", objLog)
