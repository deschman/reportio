# -*- coding: utf-8 -*-


from os.path import isfile as os_path_isfile  # ensure log is file
from datetime import datetime as dt_dt  # print current date/time
from functools import wraps  # for decorator
import logging  # process logging
import threading  # check for multithreading
from gc import collect as gc_collect  # attempt process force quit

from ..errors import LogError


__all__ = ['basicConfig', 'getThreadScope', 'debug', 'info', 'warning',
           'error', 'critical', 'log', 'decLog']


def basicConfig(filename, filemode='a',
                format='%(asctime)s %(threadName)s %(levelname)s: %(message)s',
                **kwargs):
    strNewLog = "existing"
    try:
        if not os_path_isfile(filename):
            open(filename, 'w').close()
            strNewLog = "new"
        elif filemode == 'w':
            strNewLog = 'new'
        logging.basicConfig(filename=filename, filemode=filemode,
                            format=format, level=logging.DEBUG)
        # python 3.8 and later
        # logging.basicConfig(filename=filename, filemode=filemode,
        #                     format=format, level=logging.DEBUG, force=True)
    except Exception as err:
        print(err)
        raise LogError
    return strNewLog


def getThreadScope():
    """Check if program is utilizing multithreading.

    Returns
    -------
    bolMainThread : boolean
        True if utilizing multi-threading, false if using single thread."""
    bolMainThread = threading.current_thread() is not threading.main_thread()
    return bolMainThread


def _printLog(strMsg, strLevel):
    print(dt_dt.now(), '{0}:'.format(strLevel), strMsg)


def debug(strMsg):
    """Simply calls logging.debug, passing strMsg. Added to module for full
    compatability in place of logging standard library.

    Parameters
    ----------
    strMsg : string
        Message to be logged and printed."""
    logging.debug(strMsg)


def info(strMsg):
    """Calls logging.info and prints message to console in the format of
    YYYY-MM-DD HH:MM:SS.ssssss INFO: strMsg

    Parameters
    ----------
    strMsg : string
        Message to be logged and printed."""
    logging.info(strMsg)
    _printLog(strMsg, 'INFO')


def warning(strMsg):
    """Calls logging.warning and prints message to console in the format of
    YYYY-MM-DD HH:MM:SS.ssssss WARNING: strMsg

    Parameters
    ----------
    strMsg : string
        Message to be logged and printed."""
    logging.warning(strMsg)
    _printLog(strMsg, 'WARNING')


def error(strMsg):
    """Calls logging.error and prints message to console in the format of
    YYYY-MM-DD HH:MM:SS.ssssss ERROR: strMsg

    Parameters
    ----------
    strMsg : string
        Message to be logged and printed."""
    logging.error(strMsg)
    _printLog(strMsg, 'ERROR')


def critical(strMsg):
    """Calls logging.critical and prints message to console in the format of
    YYYY-MM-DD HH:MM:SS.ssssss CRITICAL: strMsg

    Parameters
    ----------
    strMsg : string
        Message to be logged and printed."""
    logging.critical(strMsg, exc_info=True)
    _printLog(strMsg, 'CRITICAL')


def log(strMsg, strLevel='INFO'):
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
    if getThreadScope():
        strLevel = "DEBUG"
    if strLevel == 'DEBUG':
        debug(strMsg)
    elif strLevel == 'INFO':
        info(strMsg)
    elif strLevel == 'WARNING':
        warning(strMsg)
    elif strLevel == 'ERROR':
        error(strMsg)
    elif strLevel == 'CRITICAL':
        critical(strMsg)


# TODO: get this working as a decorator so that log file closes after use
# This implementation does not work. See todo fix below.
class decLog:
    def __init__(self, objMethod):
        if not callable(objMethod):
            # TODO: create this custom error
            raise Exception
        self.objMethod = objMethod

    def __get__(self, obj, type=None):
        return self.__class__(self.objMethod.__get__(obj, type))

    def __call__(self, *args, **kwargs):
        # Attempt to retrieve log directory from method
        try:
            # TODO: fix this so it does not throw an attribute error
            self.dirLog = self.objMethod.__self__.dirLog
        except AttributeError:
            # TODO: create this custom error
            raise Exception
        # Configure logger
        import logging
        basicConfig(filename=self.objMethod.__self__.dirLog,
                    level=logging.DEBUG, *args, **kwargs)
        result = self.objMethod(*args, **kwargs)
        logging.shutdown()
        del logging
        gc_collect()
        return result
