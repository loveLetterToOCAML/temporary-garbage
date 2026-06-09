from basetypes.implementation.basetypes_match import DefaultBaseType

from pydantic import BaseModel
from typing import List
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

class ChunkOrderingState(BaseModel):
    missingChunks: int | List[int] | IntervalUnion[int]
    uploadedChunks: int | List[int] | IntervalUnion[int]
    remainingTime: DefaultBaseType.TIMEDELTA | None
    currentChunkTimeMean: DefaultBaseType.TIMEDELTA
    maximumChunkTime: DefaultBaseType.TIMEDELTA


class ProposedChunkConstraint(BaseModel):
    chunkSize: int = 0x100000
    numberOfChunks: int
    withHash: bool = True

class DataChunk(BaseModel):
    index: int
    data: bytes
    integrity: Hash | None
