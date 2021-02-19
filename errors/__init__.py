# -*- coding: utf-8 -*-
"""Contains all errors used throughout reportio package."""


# %% Variables
__all__ = ['ReportError', 'LogError', 'ConfigError', 'ReportNameError',
           'DBConnectionError', 'UnexpectedDbType', 'DatasetNameError',
           'EmptyReport']


# %% Classes
class ReportError(Exception):
    """Base exception class for reporting module."""

    def __init__(self, msg: str) -> None:
        self.message = msg


class LogError(ReportError):
    """Error encountered while configuring log."""

    def __init__(self) -> None:
        super().__init__("unable to configure log")


class ConfigError(ReportError):
    """Error encountered while finding config file."""

    def __init__(self) -> None:
        super().__init__("config file not found")


class ReportNameError(ReportError):
    """Error encountered with report name."""

    def __init__(self, report_name: str) -> None:
        super().__init__("cannot use '{0}' as report name".format(report_name))


class DBConnectionError(ReportError):
    """Error encountered while connecting to database."""

    def __init__(self) -> None:
        super().__init__("connection to database failed")


class UnexpectedDbType(ReportError):
    """Error encountered attempting to find DB name in config."""

    def __init__(self, strType) -> None:
        super().__init__("database type '{0}' not excpected".format(strType))


class DatasetNameError(ReportError):
    """Error encountered with dataset name."""

    def __init__(self, dataset_name: str) -> None:
        super().__init__("cannot use '{0}' as dataset name".format(
            dataset_name))


class EmptyReport(ReportError):
    """Report contains no data."""

    def __init__(self) -> None:
        super().__init__("report contains no data")
