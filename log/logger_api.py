from __future__ import annotations

from policy.log import LogLevel

from typing import Protocol, Type, final
import contextlib


class LoggerApi(Protocol):

    def log(self, log_level: LogLevel, message: str | None = None, LogRecordType: Type | None = None, *,
            with_exc_info: bool = True, **additional_parameters):
        ...

    @contextlib.contextmanager
    def bind(self, **bound_variables):
        ...

    @final
    def debug(self, message: str | None = None, LogRecordType: Type | None = None, **additional_parameters):
        return self.log(LogLevel.DEBUG, message, LogRecordType, **additional_parameters)

    @final
    def info(self, message: str | None = None, LogRecordType: Type | None = None, **additional_parameters):
        return self.log(LogLevel.INFO, message, LogRecordType, **additional_parameters)

    @final
    def warning(self, message: str | None = None, LogRecordType: Type | None = None, **additional_parameters):
        return self.log(LogLevel.WARNING, message, LogRecordType, **additional_parameters)

    @final
    def error(self, message: str | None = None, LogRecordType: Type | None = None, **additional_parameters):
        return self.log(LogLevel.ERROR, message, LogRecordType, **additional_parameters)

    @final
    def exception(self, message: str | None = None, LogRecordType: Type | None = None, *,
                  with_exc_info: bool = True, **additional_parameters):
        return self.log(LogLevel.ERROR, message, LogRecordType, with_exc_info=with_exc_info, **additional_parameters)

    @final
    def critical(self, message: str | None = None, LogRecordType: Type | None = None, **additional_parameters):
        return self.log(LogLevel.CRITICAL, message, LogRecordType, **additional_parameters)
