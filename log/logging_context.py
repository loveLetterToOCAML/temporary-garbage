from baseimplems.anyio_utils import register_manager_on_context_update_sync
from policy.log import LogPolicy, PythonLoggingApi, current_log_policy
from log.logging_wrapping_logger import PythonLoggingWrappingLogger
from utils.custom_context_var import ContextVarWrapper
from context.init import InitContextVarException
from log.logger_api import LoggerApi

from contextvars import ContextVar
from typing import Dict, Type
import contextlib


# Main logger object which handles current logger as contextvar and register log policy change callback
#LogApiDispatcher = ProxyToApi(LoggerApi)

# TODO: proper handling of 2 classes of object: good ones (which implement proper context handling in contextvars) and
# "bad" ones (which does not implement this, and have some reset state to revert, meaning a "foreground" function which
# aims at ensuring turning back to original state when exiting the context manager)

class LogApiDispatcher(LoggerApi):

    def __init__(self, name, LoggerClass, log_policy: LogPolicy | None = None):
        self._name = name
        self._LoggerClass: Type = LoggerClass
        if log_policy:
            self._current_logger: ContextVarWrapper[LoggerApi] = \
                ContextVarWrapper(f"logger[{name}]", default=self._LoggerClass(name, log_policy))
        else:
            self._current_logger: ContextVarWrapper[LoggerApi] = ContextVarWrapper(f"logger[{name}]")

    def log(self, *args, **kwargs):
        return self._current_logger.log(*args, **kwargs)

    @contextlib.contextmanager
    def reset_log_policy(self, log_policy: LogPolicy):
        lgr = self._LoggerClass(self._name, log_policy)
        prev_obj = self._current_logger.value_or_none
        previous_logger = self._current_logger.set(lgr)
        try:
            yield lgr
        finally:
            if prev_obj:  # this is to handle special cases where proper context handling is not initially managed by the object
                prev_obj.foreground()
            self._current_logger.reset(previous_logger)


# the contextvar below is to ensure object unicity in multithread / async context so that resetting a log policy
# in one thread does not reset the log policy of another object in another thread
_registered_loggers: ContextVar[Dict[str, LogApiDispatcher]] = ContextVar('registered_loggers', default={})


@contextlib.contextmanager
def _reset_log_policy(log_policy: LogPolicy):
    with contextlib.ExitStack() as stack:
        [stack.enter_context(ctxt.reset_log_policy(log_policy)) for ctxt in _registered_loggers.get().values()]
        yield


register_manager_on_context_update_sync(current_log_policy, _reset_log_policy)


def logger_for(logger_name):
    try:
        log_policy: LogPolicy = current_log_policy.get()
    except:
        raise InitContextVarException('run_with_log_policy', current_log_policy)

    full_logger_name = f"{log_policy.logApi.apiType.name}::{logger_name}"
    registered_loggers = _registered_loggers.get()
    if full_logger_name in registered_loggers:
        return registered_loggers[full_logger_name]

    match log_policy.logApi:
        case PythonLoggingApi():
            logger = LogApiDispatcher(logger_name, PythonLoggingWrappingLogger, log_policy)
            root_logger_name = f"{log_policy.logApi.apiType.name}::"
            # avoid the case where we already define root logger
            if logger_name and root_logger_name not in registered_loggers:
                # register the root logging logger by default
                registered_loggers[root_logger_name] = LogApiDispatcher('', PythonLoggingWrappingLogger, log_policy)
        case _:
            raise NotImplementedError

    registered_loggers[full_logger_name] = logger
    return registered_loggers[full_logger_name]


if __name__ == '__main__':
    from policy.log import LogLevel, StdoutSink, StderrSink
    from policy.log import run_with_log_policy_sync

    from threading import Thread
    import time

    def run1():
        with run_with_log_policy_sync():
            lgr = logger_for(__name__)
            lgr.info("OK 1")
            time.sleep(1)
            with run_with_log_policy_sync(logLevel=LogLevel.ERROR):
                lgr.error("starting")
                time.sleep(2)
                lgr.error("ending")
            lgr.info("end")

    def run2():
        with run_with_log_policy_sync():
            lgr = logger_for(__name__)
            lgr.info("OK 2")
            time.sleep(2)
            lgr.error("always good")
            lgr.info("bad ?")
            time.sleep(2)
            lgr.info("good again ?")

    t1 = Thread(target=run1)
    t2 = Thread(target=run2)
    t1.start()
    time.sleep(0.5)
    t2.start()
    t1.join()
    t2.join()

    with run_with_log_policy_sync():
        lgr = logger_for(__name__)
        lgr.info('test')
        lgr.error('test')

        with run_with_log_policy_sync(logLevel=LogLevel.ERROR):
            lgr.info('test2')
            lgr.error('test2')

        lgr.info('test3')
        lgr.warning('test3')

        with run_with_log_policy_sync(logLevel=LogLevel.WARNING, stdoutLogSink=StderrSink(), stderrLogSink=StdoutSink()):
            lgr.warning('test4')
            lgr.error('test4')

            try:
                raise Exception("whatever")
            except Exception as _:
                lgr.exception('this is exc')

            lgr.warning('then it goes')


# stderr
# [ERROR] [__main__::stderr] 16/Mar/2026 00:30:56 test
# [ERROR] [__main__::stderr] 16/Mar/2026 00:30:56 test2
# [WARNING] [__main__::stdout] 16/Mar/2026 00:30:56 test4

# stdout
# [INFO] [__main__::stdout] 16/Mar/2026 00:30:56 test
# [INFO] [__main__::stdout] 16/Mar/2026 00:30:56 test3
# [WARNING] [__main__::stdout] 16/Mar/2026 00:30:56 test3
# [ERROR] [__main__::stderr] 16/Mar/2026 00:30:56 test4
