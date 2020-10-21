# -*- coding: utf-8 -*-


import logging  # process logging
from os.path import isfile as os_path_isfile  # ensure log is file
from gc import collect as gc_collect  # attempt process force quit
from reporting.errors import LogError

# TODO: get this working as a decorator so that log file closes after use
def decLog(fun=None):
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
        bolNewLog = False
        try:
            if not os_path_isfile(report.dirLog):
                open(report.dirLog, 'w').close()
                bolNewLog = True
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
        return bolNewLog
    return wrapper
