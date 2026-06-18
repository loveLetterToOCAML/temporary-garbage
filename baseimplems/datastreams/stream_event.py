from typing import Literal

from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root_params import RootSerial
from baseimplems.utils import utc_now

from pydantic import BaseModel

from datetime import datetime, timedelta
from enum import Enum


# rel time is relative to the beginning of the upper stream or of the current task for the first stream
class BaseEvent(BaseModel):
    absTime: datetime
    relTime: timedelta


def base_event_from(start: datetime):
    now = utc_now()
    return {
        'absTime': now,
        'relTime': now - start
    }


class StreamIdentifier(BaseModel):
    name: str
    index: int

    model_config = {"frozen": True}

    def __hash__(self):
        return hash((self.name, self.index))


class StreamEvent(BaseEvent, StreamIdentifier):
    pass

class StreamEndReason(Enum):
    END_OF_INPUT = 1
    EXTERNAL_SIGNAL = 2
    EXCEPTION_DURING_WRITE = 3


class StreamStarting(StreamEvent):
    details: RootSerial | None = None

class StreamEnding(StreamEvent):
    reason: StreamEndReason



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
