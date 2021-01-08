import logging
from logging.handlers import RotatingFileHandler
from os.path import isdir, join

LEVEL = logging.DEBUG


def start_logging(log_folder=None):
    fmt, datefmt = '[%(asctime)s.%(msecs)03d] [%(filename)s:%(lineno)s - %(funcName)s] %(message)s', '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # add the stream handler and have it manage DEBUGS
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    sh.setLevel(logging.DEBUG)
    logger.addHandler(sh)

    # add the file handlers and have them manage both DEBUG and EXCEPTION
    if isinstance(log_folder, str):
        if isdir(log_folder):
            # set the default exception log file in append mode
            exception_file = join(log_folder, 'exceptions.log')
            exception_handler = RotatingFileHandler(exception_file, backupCount=10)
            # enforce rollover
            exception_handler.doRollover()
            # set formatters and levels
            exception_handler.setFormatter(formatter)
            exception_handler.setLevel(logging.ERROR)
            # add handler
            logger.addHandler(exception_handler)

            # set the default debug log file in append mode
            debug_file = join(log_folder, 'debugs.log')
            debug_handler = RotatingFileHandler(debug_file, backupCount=10)
            # enforce rollover
            debug_handler.doRollover()
            debug_handler.setFormatter(formatter)
            # set formatters and levels
            debug_handler.setFormatter(formatter)
            debug_handler.setLevel(logging.DEBUG)
            # add handler
            logger.addHandler(debug_handler)
