# -*- coding: utf-8 -*-
from __future__ import print_function, division, unicode_literals

import logging
import logging.handlers
import os


def make_sure_path_exists(dest_path):
    if not os.path.isfile(dest_path):
        dest_folder = os.path.dirname(dest_path)

        if not os.path.exists(dest_folder):
            try:
                os.makedirs(dest_folder)
            except OSError as e: # Guard against race condition
                if e.errno != errno.EEXIST:
                    raise
        return False
    else:
        return True


class Log(object):

    def __init__(self, logger=None):
        self.logging_initialized = False
        self.levels = dict()
        self.handlers = dict()
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()

    def debug(self, handler_id=None, *args, **kargs):
        return self.trace(logging.DEBUG, handler_id, *args, **kargs)

    def info(self, handler_id=None, *args, **kargs):
        return self.trace(logging.INFO, handler_id, *args, **kargs)

    def warning(self, handler_id=None, *args, **kargs):
        return self.trace(logging.WARNING, handler_id, *args, **kargs)

    def error(self, handler_id=None, *args, **kargs):
        return self.trace(logging.ERROR, handler_id, *args, **kargs)

    def critical(self, handler_id=None, *args, **kargs):
        return self.trace(logging.CRITICAL, handler_id, *args, **kargs)

    def kickoff_logging(self):
        if not self.logging_initialized:

            formater = logging.Formatter(
                '[%(asctime).19s,%(msecs)03d] %(levelname)s %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')

            # Mitigate the effects of a recent bug in python 3.6
            number_of_handlers = len(self.handlers.keys())

            for handler_id in self.handlers:
                handler = self.handlers[handler_id]
                level = self.levels[handler_id]

                handler.setFormatter(formater)
                if level != logging.NOTSET:
                    handler.setLevel(level)
                    # Mitigate the effects of a recent bug in python 3.6
                    if number_of_handlers == 1:
                        self.logger.setLevel(level)
                else:
                    handler.setLevel(logging.ERROR)
                    # Mitigate the effects of a recent bug in python 3.6
                    if number_of_handlers == 1:
                        self.logger.setLevel(logging.ERROR)
                self.logger.addHandler(handler)

            if None not in self.handlers:
                handler = logging.StreamHandler()
                handler.setLevel(logging.CRITICAL)
                self.logger.addHandler(handler)

            # Mitigate the effects of a recent bug in python 3.6
            if number_of_handlers == 0:
                self.logger.setLevel(logging.CRITICAL)

        self.logging_initialized = True
        return self

    def trace(self, level=None, handler_id=None, *args, **kargs):
        if handler_id:
            make_sure_path_exists(handler_id)

            self.handlers[handler_id] = logging.handlers.RotatingFileHandler(
                handler_id, maxBytes=1024**2, backupCount=9)

        else:
            self.handlers[handler_id] = logging.StreamHandler()

        if level is not None:
            self.levels[handler_id] = level
        else:
            self.levels[handler_id] = logging.NOTSET

        return self

    def close(self):
        for handler_id in self.handlers:
            handler = self.handlers[handler_id]
            handler.close()
            self.logger.removeHandler(handler)

log = Log()
logger = log.logger
