# -*- coding: utf-8 -*-


__all__ = ['ReportError', 'LogError', 'ConfigError', 'ReportNameError',
           'DBConnectionError', 'UnexpectedDbType', 'DatasetNameError',
           'EmptyReport']


class ReportError(Exception):
    def __init__(self, strMsg):
        """Base exception class for reporting module. Will call log object if
        it is a function, otherwise will print message and try to call
        .critical. Will always return message.

        Parameters
        ----------
        strMsg : string
            Error message.

        Returns
        -------
        Message : string
            Pass through from error call."""
        self.message = strMsg


class LogError(ReportError):
    def __init__(self):
        """Error encountered while configuring log for report."""
        strMsg = "unable to configure log"
        self.message = strMsg


class ConfigError(ReportError):
    def __init__(self):
        super().__init__("config file not found")


class ReportNameError(ReportError):
    def __init__(self, strName):
        super().__init__("cannot use '{0}' as report name".format(strName))


class DBConnectionError(ReportError):
    def __init__(self):
        super().__init__("connection to database failed")


class UnexpectedDbType(ReportError):
    def __init__(self, strType):
        super().__init__("database type '{0}' not excpected".format(strType))


class DatasetNameError(ReportError):
    def __init__(self, strName):
        super().__init__("cannot use '{0}' as dataset name")


class EmptyReport(ReportError):
    def __init__(self):
        super().__init__("report contains no data")
