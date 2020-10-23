# -*- coding: utf-8 -*-


class ReportError(Exception):
    def __init__(self, strMsg, objLog):
        """Base exception class for reporting module. Will call log object if
        it is a function, otherwise will print message and try to call
        .critical. Will always return message.

        Parameters
        ----------
        strMsg : string
            Error message.
        objLog : {function, object}
            Function for logging or logging object from logging module.

        Returns
        -------
        Message : string
            Pass through from error call."""
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
        super().__init__("config file not found", objLog)


class ReportNameError(ReportError):
    def __init__(self, objLog, strName):
        super().__init__("cannot use '{0}' as report name".format(strName),
                         objLog)


class ODBCConnectionError(ReportError):
    def __init__(self, objLog):
        super().__init__("connection to database failed", objLog)


class UnexpectedDbType(ReportError):
    def __init__(self, objLog, strType):
        super().__init__("database type '{0}' not excpected".format(strType),
                         objLog)


class DatasetNameError(ReportError):
    def __init__(self, objLog, strName):
        super().__init__("cannot use '{0}' as dataset name", objLog)


class EmptyReport(ReportError):
    def __init__(self, objLog):
        super().__init__("report contains no queries", objLog)
