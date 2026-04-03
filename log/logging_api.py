from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root_params import RootSerial
from basetypes.implementation.generics_match import DefaultGenericType
from utils.enum_name_serializer import SerializableEnum
from policy.context_utils import run_with_policy
from basetypes.ab_basetypes import BaseDataType
from basetypes.a_root import Serial

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

GenericLogEvent = alias_for(DefaultGenericType.DICT[str, RootSerial], keyTypeHint=str, valueTypeHint=RootSerial)

# event log intent is supposed to be emitted before the log component deals with policies that handles automatic
# timing and space resolution of the event
class EventLogIntent(BaseModel):
    message: DefaultBaseType.STRING | DefaultBaseType.NONE
    eventPriority: LogLevel
    eventData: RootSerial  # EventLog is a Generic and should be serializable data
    additionalData: GenericLogEvent


class ExecutionSystem(BaseModel):
    nodeType: DefaultBaseType.TYPE
    version: DefaultBaseType.SEMANTIC_VERSION


class EventLocation(BaseModel):
    executionSystemLocation: ExecutionSystem  # on which currently execution system the log event has been emitted
    sourceCodeLocation: SourceCodeLocation    # where within the source code does the log originate from
    dynamicExecutionLocation: FiberLocation   # from which thread / fiber / process the log has been emitted (OS)

class EventLog(EventLogIntent):  # for avoiding repeating the message, priority and data
    eventUtcDate: DefaultBaseType.DATETIME  # fixing datetime format to full UTC datetime
    eventLocation: EventLocation            # fixing location format

# The event log can be extended to a generic for aliases, fixing the TYPE field
# As instance: AuthorizeLogEvent = EventLogGeneric[AuthorizeLog]
class EventLogGeneric(EventLog):
    eventLogDataType: DefaultBaseType.TYPE

class LogSinkType(Enum):
    STDOUT = 1
    STDERR = 2
    # TODO: implement RFC log sinks (syslog first), and multiple handlers logger

class LogApiType(Enum):
    PYTHON_LOGGING = 1
    # TODO: implement other useful logging API


class ResolveLocationPolicy(Enum):
    resolveExecutionSystem: bool = False
    resolveSourceCodeLocation: bool = True
    resolveDynamicExecutionLocation: bool = True


class LogRemainingArgumentsPolicy(Enum):
    IGNORE = 1
    RAISE = 2
    EMIT_WARNING = 3
    ADD_AS_ADDITIONAL_DATA = 4
