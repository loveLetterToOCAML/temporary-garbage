from __future__ import annotations

from typing import Protocol


class LoggerApi(Protocol):

    def debug(self, msg, *args, **kwargs):
        ...

    def info(self, msg, *args, **kwargs):
        ...

    def warning(self, msg, *args, **kwargs):
        ...

    def error(self, msg, *args, **kwargs):
        ...

    def exception(self, msg, *args, exc_info=True, **kwargs):
        ...

    def critical(self, msg, *args, **kwargs):
        ...
