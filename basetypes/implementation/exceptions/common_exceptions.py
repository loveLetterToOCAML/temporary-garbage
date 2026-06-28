from basetypes.ae_interaction import Interaction, InteractionType
from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root import SerializationNode, SerialType, Root
from basetypes.ab_basetypes import attempt_serial_type
from basetypes.a_root_params import RootSerial

from typing import Type, Protocol
from pydantic import BaseModel
from types import UnionType
from enum import Enum


# most of the exceptions that can be encountered internally by the current piece of code only (not dependencies)
# other and more specific exceptions can / should be added within the other section
class CommonExceptionType(Enum):
    ExecutionFailure = 1
    ExpectedType = 2
    Timeout = 3
    GenericException = 0x80
    ExceptionWithStacktrace = 0x81
    Other = 0xff

### BEGIN AUTO GENERATION
# Auto-generated from CommonExceptionType for auto-completion purpose
class CommonException(SerializationNode):
    ExecutionFailure = ...
    ExpectedType = ...
    Timeout = ...
    GenericException = ...
    ExceptionWithStacktrace = ...
    Other = ...
### END AUTO GENERATION


CommonExceptions: CommonException = Interaction.register_serialization_child(InteractionType.Exception, CommonExceptionType)


class HumanReadableExceptionModel(BaseModel):
    message: str | None
    stacktrace: None = None # | Stacktrace

class ExpectedTypeExceptionModel(BaseModel):
    got: DefaultBaseType.TYPE | str
    expected: DefaultBaseType.TYPE | str


CommonExceptions.register_serialization_leaf(CommonExceptionType.GenericException, HumanReadableExceptionModel)
CommonExceptions.register_serialization_leaf(CommonExceptionType.ExpectedType, ExpectedTypeExceptionModel)
SerialHumanReadableException: HumanReadableExceptionModel = CommonExceptions.leaf_object_constructor(CommonExceptionType.GenericException)
SerialExpectedTypeException: ExpectedTypeExceptionModel = CommonExceptions.leaf_object_constructor(CommonExceptionType.ExpectedType)

exception_types_match = {}

def link_runtime_to_serializable(PythonExceptionType, SerializableExceptionType):
    exception_types_match[PythonExceptionType] = SerializableExceptionType

def attempt_serial_exception(exn: Exception):
    return exception_types_match[type(exn)](**exn.__dict__)


class HumanReadableException(Exception):

    def __init__(self, *args):
        super().__init__(*args)
        self.message = f"{self}"


class ExpectedTypeException(Exception):

    def __init__(self, got: Type, expected: Type | UnionType):
        expected_msg = ' | '.join(map(lambda t: t.__name__, expected.__args__)) if isinstance(expected, UnionType) \
            else expected.__name__
        exn_msg = f"Expected type {expected_msg}, got {got.__name__}"
        super().__init__(exn_msg)
        self.expected = attempt_serial_type(expected) or expected_msg
        self.got = attempt_serial_type(got) or got.__name__


link_runtime_to_serializable(HumanReadableException, SerialHumanReadableException)
link_runtime_to_serializable(ExpectedTypeException, SerialExpectedTypeException)


if __name__ == '__main__':
    e = ExpectedTypeException(got=int, expected=str)
    print(e)
    f = ExpectedTypeException(got=int, expected=str | dict)
    print(f)

    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(CommonExceptionType))

    print(attempt_serial_exception(ExpectedTypeException(got=int, expected=str | dict)))
