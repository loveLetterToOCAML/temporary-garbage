from enum import Enum

from basetypes.implementation.basetypes_match import DefaultBaseType

from pydantic import BaseModel
from typing import List, Generic, TypeVar
import datetime


class GlobalChunksConstraint(BaseModel):
    maximumChunkSize: int = 0x100000   # 1 Mb chunks
    maximumChunks: int = 0x100000      # squared : 1 Tb data max

class TimedGlobalChunksConstraint(GlobalChunksConstraint):
    maximumTotalTime: DefaultBaseType.TIMEDELTA = datetime.timedelta(seconds=-1)  # negative means no maximum delay
    maximumChunkTime: DefaultBaseType.TIMEDELTA = datetime.timedelta(seconds=1)
    waitBeforeCount: DefaultBaseType.TIMEDELTA = datetime.timedelta(seconds=10)
    bonusTimePer0x1000Chunks: DefaultBaseType.TIMEDELTA = datetime.timedelta(seconds=120)
    allowIfAnyMax: bool = True  # in the true case, even if maximum total size is set, it will continue even if elapsed while maximumChunkTime is respected


# Chunking = Content-addressed transfer  Content digest / commitment (manifest-first)
# Or
# Chunking = Streaming transfer  Trailing checksum / trailer digest

# Chunking is a way to cut and send large amount of data in small pieces.

# ContentTransfer is only compatible with knowing the exact data to transmit and its size, which can only happen in
# non-streaming cases. Otherwise if we add some hash of produced data that must be sent prior to data, "replaying"
# data is mandatory so all must be kept somewhere

# StreamingTransfer is to use when prior size / content is unknown. ContentTransfer should be preferred in most cases


T = TypeVar('T')

class IntervalUnion(BaseModel, Generic[T]):
    pass

class StreamingTransferState(BaseModel):
    receivedChunks: IntervalUnion[int]
    currentChunkTimeMean: DefaultBaseType.TIMEDELTA
    maximumChunkTime: DefaultBaseType.TIMEDELTA

class ContentTransferState(StreamingTransferState):
    missingChunks: List[int] | IntervalUnion[int]
    remainingTimeEstimation: DefaultBaseType.TIMEDELTA | None


class ChecksumType(Enum):
    NONE = 1
    CRC32 = 2
    CRC64 = 3

class StreamingTransferParameters(BaseModel):
    chunkSize: int = 0x100000
    withChecksum: ChecksumType = ChecksumType.CRC32

class ContentTransferParameters(StreamingTransferParameters):
    numberOfChunks: int

class DataChunk(BaseModel):
    index: int
    data: bytes
    offset: int
    integrity: bytes | None
