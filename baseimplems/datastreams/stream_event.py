from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root_params import RootSerial
from baseimplems.date_utils import utc_now

from pydantic import BaseModel

from datetime import datetime, timedelta
from typing import Literal
from enum import Enum


# rel time is relative to the beginning of the upper stream or of the current task for the first stream
class BaseEvent(BaseModel):
    relTime: timedelta
    absTime: datetime


def base_event_from(start: datetime | None = None):
    now = utc_now()
    return {
        'absTime': now,
        'relTime': (now - start) if start else timedelta(0)
    }


class StreamIdentifier(BaseModel):
    name: str
    index: int
    randomId: int  # this is used to ensure unicity of streams globally event with multiple event manager at same time

    model_config = {"frozen": True}

    def __hash__(self):
        return hash((self.name, self.index, self.randomId))


class StreamEvent(BaseEvent, StreamIdentifier):
    pass

class StreamEndReason(Enum):
    END_OF_INPUT = 1      # normal termination of stream
    EXTERNAL_SIGNAL = 2   # external upper cancellation (timeout or any cancel)
    EXCEPTION_DURING_PROCESS = 3   # "internal" cancellation from below processing streams


class StreamStarting(StreamEvent):
    details: RootSerial | None = None

class StreamInProgress(StreamEvent):
    pass

class StreamEnding(StreamEvent):
    reason: StreamEndReason
    details: RootSerial | BaseModel | str | None = None


class TransitStatus(Enum):
    DONE = 1
    QUEUED = 2
    SENDING = 3
    FAILED = 4
    RETRYING = 5

class WithTransitInformation(StreamEvent):
    status: TransitStatus
    attempt: int = 1
    details: RootSerial | None = None


class StreamEventType(Enum):
    BYTES_EVENT = 1
    CHUNK_EVENT = 2
    OBJECT_EVENT = 3
    SLEEP_EVENT = 4


class BytesStreamEvent(WithTransitInformation):
    type: Literal[StreamEventType.BYTES_EVENT] = StreamEventType.BYTES_EVENT
    offset: int
    size: int

class ChunkStreamEvent(WithTransitInformation):
    type: Literal[StreamEventType.CHUNK_EVENT] = StreamEventType.CHUNK_EVENT
    index: int
    offset: int
    size: int

class ObjectStreamEvent(WithTransitInformation):
    type: Literal[StreamEventType.OBJECT_EVENT] = StreamEventType.OBJECT_EVENT
    index: int
    type: DefaultBaseType.TYPE


class SleepEvent(StreamEvent):
    type: Literal[StreamEventType.SLEEP_EVENT] = StreamEventType.SLEEP_EVENT
    delay: DefaultBaseType.TIMEDELTA


def event_payload_size(stream_event: StreamEvent):
    match stream_event:
        case BytesStreamEvent():
            return stream_event.size
        case ChunkStreamEvent():
            return stream_event.size
        case ObjectStreamEvent():
            return 1
        case SleepEvent():
            return 0
        case _:
            raise NotImplementedError
