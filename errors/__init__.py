# -*- coding: utf-8 -*-


__all__ = ['ReportError', 'LogError', 'ConfigError', 'ReportNameError',
           'DBConnectionError', 'UnexpectedDbType', 'DatasetNameError',
           'EmptyReport']


class ReportError(Exception):
    def __init__(self, msg: str) -> None:
        """
        Base exception class for reporting module. Will call log object if
        it is a function, otherwise will print message and try to call
        .critical. Will always return message.

        Parameters
        ----------
        msg : string
            Error message.
        """
        self.message = msg


class LogError(ReportError):
    def __init__(self) -> None:
        """
        Error encountered while configuring log.
        """
        super().__init__("unable to configure log")


class ConfigError(ReportError):
    def __init__(self) -> None:
        """
        Error encountered while finding config file.
        """
        super().__init__("config file not found")


class ReportNameError(ReportError):
    def __init__(self, report_name: str) -> None:
        """
        Error encountered with report name.
        """
        super().__init__("cannot use '{0}' as report name".format(report_name))


class DBConnectionError(ReportError):
    def __init__(self) -> None:
        """
        Error encountered while connecting to database.
        """
        super().__init__("connection to database failed")


class UnexpectedDbType(ReportError):
    def __init__(self, strType) -> None:
        """
        Error encountered attempting to find DB name in config.
        """
        super().__init__("database type '{0}' not excpected".format(strType))


class DatasetNameError(ReportError):
    def __init__(self, dataset_name: str) -> None:
        """
        Error encountered with dataset name.
        """
        super().__init__("cannot use '{0}' as dataset name".format(
            dataset_name))


class EmptyReport(ReportError):
    def __init__(self) -> None:
        """
        Report is empty.
        """
        super().__init__("report contains no data")
