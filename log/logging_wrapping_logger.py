from policy.log import LogPolicy, LogSink, StderrSink, StdoutSink, PythonLoggingFormatting, LogApiType
from log.logger_api import LoggerApi

from threading import Lock, currentThread
from functools import wraps
import logging
import sys


# TODO: safe lock the dict? Check if needed, as it's append-only struct
# We consider there should not be a lot of handlers being declared so we can keep them during the full program execution
_handlers_per_type = {}   # handlers are supposed to be state free so share them

def new_or_existing_handler(log_sink: LogSink, formatter: PythonLoggingFormatting):
    match log_sink:
        case StdoutSink() | StderrSink():
            key = (log_sink.logSinkType, formatter.logFormat, formatter.dateFormatter)
        case _:
            raise NotImplementedError

    if key in _handlers_per_type:
        return _handlers_per_type[key]

    match log_sink:
        case StdoutSink():
            _handlers_per_type[key] = logging.StreamHandler(stream=sys.stdout)
        case StderrSink():
            _handlers_per_type[key] = logging.StreamHandler(stream=sys.stderr)
        case _:
            raise NotImplementedError

    formatter = logging.Formatter(formatter.logFormat, datefmt=formatter.dateFormatter)
    _handlers_per_type[key].setFormatter(formatter)

    return _handlers_per_type[key]


def under_lock_call(f):
    @wraps(f)
    def sub(self, *args, **kwargs):
        if self._ensure_thread_lock:
            with self._log_lock:
                f(self, *args, **kwargs)
        else:
            f(self, *args, **kwargs)
    return sub

# This handles all side effects of logging, by also using the contextvar containing current logging policy
class PythonLoggingWrappingLogger(LoggerApi):

    # this wrapper should never be created directly by devs, only called automatically by the logger.py file
    def __init__(self, name: str, log_policy: LogPolicy):
        if log_policy.logApi.apiType != LogApiType.PYTHON_LOGGING:  # log_policy.logApi.apiParameters is PythonLoggingFormatting
            raise Exception(f'Log policy {log_policy.logApi.apiType} not supported when constructing '
                            f'PythonLoggingWrappingLogger')
        stdout_n = f"{name}::{currentThread().ident}::stdout"
        stderr_n = f"{name}::{currentThread().ident}::stderr"
        self._name = name
        self._log_policy = log_policy

        self._stdout_logger = logging.getLogger(stdout_n)
        self._stderr_logger = logging.getLogger(stderr_n)
        # start by cleaning potentially already defined handlers
        self._stdout_logger.propagate = False
        self._stderr_logger.propagate = False
        self._ensure_thread_lock = False

        level = log_policy.logLevel.value  # we matched the same API as logging
        # as using logging getLogger (sharing loggers for a name) we must reset to original state when the object is
        # destroyed TODO: ensure it's thread safe and globally safe due to garbage collection (does it happens immediately?)
        self._initial_stdout_level = self._stdout_logger.getEffectiveLevel()
        self._initial_stderr_level = self._stderr_logger.getEffectiveLevel()
        self._stdout_logger.setLevel(level)
        self._stderr_logger.setLevel(level)

        self._stdout_h = new_or_existing_handler(log_policy.stdoutLogSink, log_policy.logApi.apiParameters)
        self._stderr_h = new_or_existing_handler(log_policy.stderrLogSink, log_policy.logApi.apiParameters)
        self.foreground()

        self._ensure_thread_lock = log_policy.threadSafeLog
        self._log_lock = Lock()  # this makes us depending on threading features (no async); TODO: check what can be done

    def foreground(self):
        self._stdout_logger.setLevel(self._log_policy.logLevel.value)
        self._stderr_logger.setLevel(self._log_policy.logLevel.value)
        for handler in self._stdout_logger.handlers:
            self._stdout_logger.removeHandler(handler)
        for handler in self._stderr_logger.handlers:
            self._stderr_logger.removeHandler(handler)
        self._stdout_logger.addHandler(self._stdout_h)
        self._stderr_logger.addHandler(self._stderr_h)

    @under_lock_call
    def debug(self, msg, *args, **kwargs):
        self._stdout_logger.debug(msg, *args, **kwargs)

    @under_lock_call
    def info(self, msg, *args, **kwargs):
        self._stdout_logger.info(msg, *args, **kwargs)

    @under_lock_call
    def warning(self, msg, *args, **kwargs):
        self._stdout_logger.warning(msg, *args, **kwargs)

    @under_lock_call
    def error(self, msg, *args, **kwargs):
        self._stderr_logger.error(msg, *args, **kwargs)

    @under_lock_call
    def exception(self, msg, *args, exc_info=True, **kwargs):
        self._stderr_logger.error(msg, *args, exc_info=exc_info, **kwargs)

    @under_lock_call
    def critical(self, msg, *args, **kwargs):
        self._stderr_logger.critical(msg, *args, **kwargs)

    def __repr__(self):
        return '<%s %s (%s)> + <%s %s (%s)>' % (
            self.__class__.__name__, self._stdout_logger.name, logging.getLevelName(self._stdout_logger.getEffectiveLevel()),
            self.__class__.__name__, self._stderr_logger.name, logging.getLevelName(self._stderr_logger.getEffectiveLevel()),
        )
