import logging
import os
import time


DEFAULT_LOG_DIR = 'log/nauts'

# FORMAT = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'
# FORMAT = '%(asctime)-15s %(user)-8s %(message)s'
FORMAT = '%(asctime)-15s %(message)s'
FORMATTER = logging.Formatter(FORMAT)


def init_logging_handler_for_klass(klass, level=logging.INFO, include_time2file=False):

    file_path = os.path.realpath(klass.__module__.replace('.', '/'))
    name = klass.__name__

    module_dir = os.path.dirname(os.path.realpath(file_path))
    log_dir = os.path.abspath(os.path.join(module_dir, "..", DEFAULT_LOG_DIR))

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if include_time2file is True:
        current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        log_filename = f'{current_time}_{name}'
    else:
        log_filename = name 

    file_handler = logging.FileHandler('{}/{}.log'.format(log_dir, log_filename))
    file_handler.setFormatter(FORMATTER)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(FORMATTER)

    logger = logging.getLogger(klass.__name__)
    logger.addHandler(stderr_handler)
    logger.addHandler(file_handler)
    logger.setLevel(level)

    return logger


def init_logging_handler(log_dir=DEFAULT_LOG_DIR, level=logging.INFO, name="nauts", include_time2file=False):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    if include_time2file is True:
        current_time = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
        log_filename = f'{current_time}_{name}'
    else:
        log_filename = name

    file_handler = logging.FileHandler('{}/{}.log'.format(log_dir, log_filename))
    file_handler.setFormatter(FORMATTER)

    stderr_handler = logging.StreamHandler()
    stderr_handler.setFormatter(FORMATTER)

    logger = logging.getLogger(log_filename)
    logger.addHandler(stderr_handler)
    logger.addHandler(file_handler)
    logger.setLevel(level)

    return logger
