# from https://github.com/python/cpython/blob/main/Lib/logging/__init__.py
# and https://github.com/hynek/structlog/blob/main/src/structlog/_frames.py

from __future__ import annotations

import sys
import traceback
from enum import Enum

from io import StringIO
from types import FrameType, TracebackType
from typing import Callable

from pydantic import BaseModel
from typing_extensions import Generic, TypeVar

from basetypes.a_root import SerializationNode, Root, SerialType
from basetypes.a_root_params import RootSerial
from basetypes.implementation.basetypes_match import DefaultBaseType


# in case the received exception is not derived from the serialization model, we can still match it to this simplified
# and serializable view
class ExceptionDetails(BaseModel):
    exceptionType: str | DefaultBaseType.TYPE
    exceptionArguments: list[str | RootSerial]

class SourceCodeLocation(BaseModel):
    relativeFilePath: str
    moduleName: str
    functionName: str
    lineNumber: int

    traceback: list[str] = []
    exception: ExceptionDetails | DefaultBaseType.TYPE | RootSerial | None = None


def _get_stringio_lines(call_with_stringio):
    lines = []
    class WriteSupport:
        @staticmethod
        def write(data):
            if data:
                lines.append(data if data[-1] != '\n' else data[:-1])
    call_with_stringio(WriteSupport)
    return lines


ExceptionInfoType = tuple[type[BaseException], BaseException, TracebackType | None]

def current_exception_info() -> ExceptionInfoType:
    return sys.exc_info()

def exception_info_traceback(exc_info: ExceptionInfoType, limit: int | None = None) -> list[str]:
    lines = _get_stringio_lines(
        lambda WriteSupport: traceback.print_exception(exc_info[0], exc_info[1], exc_info[2], limit, WriteSupport)
    )
    return lines

def exception_info_traceback_as_str(exc_info: ExceptionInfoType, limit: int | None = None) -> str:
    return '\n'.join(exception_info_traceback(exc_info, limit))

def exception_info_to_parsed_exception() -> ExceptionDetails | DefaultBaseType.TYPE | RootSerial | None:
    ExcType, exc, traceback = sys.exc_info()
    if len(exc.args) > 1 or len(exc.args) < 1:
        return ExceptionDetails(exceptionType=ExcType.__name__, exceptionArguments=exc.args)

    if isinstance(exc.args[0], SerializableException):
        # we have type (which is serializable), so either we already have the auto-resolved Type and we don't have anything to do
        # or maybe we should find the type within the exception type registry and aliases
        return exc.args[0] if exc.args[0].model_fields else exc.args[0].Type

    return SerializableExceptionRoot.path_until(exc.args[0].Type)


class SerializableException(RootSerial):
    pass


class BaseSerializableException(SerializableException):
    pass

T = TypeVar('T')
class GenericSerializableException(SerializableException, Generic[T]):
    namespace: DefaultBaseType.TYPE  # must match T serializable type object

class AliasedSerializableException(SerializableException):
    pass


class SerializableRuntimeException(BaseSerializableException):
    arg1: int
    arg2: float

class ExceptionType(Enum):
    RUNTIME_EXCEPTION = 1
    BAD_THING = 2

# selfExecution::Exceptions
#  |-> BaseException
#  |-> GenericException[T]
#       |-> CommonExecutionSystemExc[T]  # where T matches root serialization children (ex: GenericException[Config]), if an exception is emitted from a certain location in base types it emits this if unknown
#  |-> AliasedException  # links to all known exceptions registered with the specific register_serializable_exception method
# In other execution systems
#   AnySystem::Exception (no generic except very specific cases, base exceptions OK, almost no alias, but AliasedException in selfExecution points to this)

SerExc = Root.register_serialization_child(SerialType.SelfExecution, ExceptionType)
print(SerExc)
print(SerExc.path_until())

then = SerExc.register_serialization_child(ExceptionType.RUNTIME_EXCEPTION, SerializableRuntimeException)

class SpecificConfigException(BaseSerializableException):
    configExc: str


#then2 = SerExc.register_serialization_exception_alias(AliasExceptionType.SpecificConfigException, SpecificConfigException)


if hasattr(sys, "_getframe"):
    current_frame = lambda: sys._getframe(1)
else:
    def current_frame():
        try:
            raise Exception
        except Exception as exc:
            return exc.__traceback__.tb_frame.f_back


def frame_traceback(frame: FrameType, limit: int | None = None) -> list[str]:
    lines = _get_stringio_lines(
        lambda WriteSupport: traceback.print_stack(frame, limit, file=WriteSupport)
    )
    return lines

try:
    raise Exception("aaa", 1)
except Exception as e:
    print(sys.exc_info())
    print(exception_info_traceback_as_str(sys.exc_info()))
    print(current_frame())

    print('\n'.join(frame_traceback(current_frame())))

