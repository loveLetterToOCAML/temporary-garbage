from baseimplems.anyio_utils import run_within, run_within_sync
from utils.enum_name_serializer import SerializableEnum

from pydantic import BaseModel

from contextvars import ContextVar
from enum import Enum


# take same values as within the logging python package to avoid confusion
class LogLevel(Enum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

class LogSinkType(Enum):
    STDOUT = 1
    STDERR = 2
    # TODO: implement RFC log sinks (syslog first), and multiple handlers logger

class LogApiType(Enum):
    PYTHON_LOGGING = 1
    # TODO: implement other useful logging API


class LogSink(BaseModel):
    logSinkType: SerializableEnum(LogSinkType)

class StdoutSink(LogSink):
    logSinkType: SerializableEnum(LogSinkType) = LogSinkType.STDOUT

class StderrSink(LogSink):
    logSinkType: SerializableEnum(LogSinkType) = LogSinkType.STDERR


class LogApi(BaseModel):
    apiType: SerializableEnum(LogApiType)
    apiParameters: BaseModel


class PythonLoggingFormatting(BaseModel):
    logFormat: str = '[%(levelname)s] [%(name)s] %(asctime)s %(message)s'
    dateFormatter: str = '%d/%b/%Y %H:%M:%S'

class PythonLoggingApi(LogApi):
    apiType: SerializableEnum(LogApiType) = LogApiType.PYTHON_LOGGING
    apiParameters: PythonLoggingFormatting = PythonLoggingFormatting()


class LogPolicy(BaseModel):
    logLevel: SerializableEnum(LogLevel) = LogLevel.INFO
    # until LogLevel.WARNING all information logged is sent to stdoutLogSink when using PythonLoggingWrappingLogger
    stdoutLogSink: LogSink = StdoutSink()
    # after LogLevel.ERROR all information logged is sent to stderrLogSink when using PythonLoggingWrappingLogger
    stderrLogSink: LogSink = StderrSink()
    logApi: LogApi = PythonLoggingApi()
    threadSafeLog: bool = True


class WithLogPolicy(BaseModel):
    logPolicy: LogPolicy | None = None  # None is for when the current log policy context variable is used


current_log_policy: ContextVar[LogPolicy] = ContextVar('log_policy')
run_with_log_policy = run_within(LogPolicy, current_log_policy)
run_with_log_policy_sync = run_within_sync(LogPolicy, current_log_policy)


if __name__ == '__main__':
    lp = LogPolicy(
        logLevel=LogLevel(10),
        stdoutLogSink=LogSink(logSinkType=LogSinkType.STDERR),
        stderrLogSink=LogSink(logSinkType='STDERR')
    )
    print(lp)

    async def test():
        async with run_with_log_policy(
                logLevel=LogLevel(20),
                stdoutLogSink=LogSink(logSinkType=LogSinkType.STDERR),
                stderrLogSink=LogSink(logSinkType='STDERR')
        ) as dyn_lp:
            print(dyn_lp)
    import anyio
    anyio.run(test)
