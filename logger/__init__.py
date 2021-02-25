# -*- coding: utf-8 -*-
# TODO: consider refactoring - loggers should never be instantiated directly
"""Contains all objects from logging standard module with minor tweaks."""


# %% Imports
# %%% Py3 Standard
import os
from logging import *
import threading

# %%% User-Defined
from reportio.errors import LogError


# %% Variables
_Logger = getLogger(__name__)


# %% Functions
# %%% Private
def _log(strMsg, strLevel='INFO') -> None:
    """
    Log feedback to log file and to console as needed.

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
        'CRITICAL' : Prints to log file and console. Prints system error.
        Script/config edits are needed to fix.
    """
    # Prevents spam by redirecting log level to debug when multithreading
    if get_thread_scope():
        strLevel = "DEBUG"
    if strLevel == 'DEBUG':
        _Logger.debug(strMsg)
    elif strLevel == 'INFO':
        _Logger.info(strMsg)
    elif strLevel == 'WARNING':
        _Logger.warning(strMsg)
    elif strLevel == 'ERROR':
        _Logger.error(strMsg)
    elif strLevel == 'CRITICAL':
        _Logger.critical(strMsg, exc_info=True)


# %%% Public
def config(
        __Logger: Logger,
        file_name: str,
        file_mode: str = 'a',
        format: str = '%(asctime)s %(threadName)s %(levelname)s: %(message)s'
        ) -> str:
    """
    Create initial logger configuration.

    Running multiple times may initialize multiple loggers.

    Parameters
    ----------
    __Logger : logging.Logger
        Base Logger class object.
    file_name : str
        Name of file where log will print.
    file_mode : str, optional
        {'w': write, 'a': append}. The default is 'a'.
    format : str, optional
        Details that will print with every log message. The default is
        '%(asctime)s %(threadName)s %(levelname)s: %(message)s'.

    Returns
    -------
    new_log : str
        Indicator of whether a new log was created or not.
    """
    new_log: str = "existing"
    try:
        __Logger.setLevel(DEBUG)
        # Create file for file handler if needed
        if file_mode == 'w':
            os.remove(file_name)
        if not os.path.isfile(file_name):
            open(file_name, 'w').close()
            new_log = "new"
        # Add file handler
        _Formatter: Formatter = Formatter(format)
        _FileHandler: FileHandler = FileHandler(filename=file_name)
        _FileHandler.setLevel(DEBUG)
        _FileHandler.setFormatter(_Formatter)
        __Logger.addHandler(_FileHandler)
        # Add stream handler to print everything but debug messages to console
        thread_format: str = '%(threadName)s'
        if thread_format in format:
            format.replace(thread_format, '')
        _Formatter = Formatter(format.replace('%(threadName)s', ''))
        _StreamHandler = StreamHandler()
        _StreamHandler.setLevel(INFO)
        _StreamHandler.setFormatter(_Formatter)
        __Logger.addHandler(_StreamHandler)
        # Set global logger object
        global _Logger
        _Logger = __Logger
    except Exception:
        raise LogError
    return new_log


def get_thread_scope() -> None:
    """
    Check if program is utilizing multithreading.

    Returns
    -------
    boolean
        True if utilizing multi-threading, false if using single thread.
    """
    return threading.current_thread() is not threading.main_thread()


# %% Script
log = _log
