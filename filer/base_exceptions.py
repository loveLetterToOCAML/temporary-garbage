from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.filer_type_registration import FilerInteractions
from basetypes.ae_interaction import InteractionType

from pydantic import BaseModel

from enum import Enum


class FilerExceptionType(Enum):
    NotExistingContent = 1
    HashNotMatchingContent = 2
    AlreadyUploadedContent = 3
    NotEnoughSpaceRemaining = 4
    OutOfSpaceConstraints = 5
    AlreadyUploadedChunk = 6
    BadChunkUploaded = 7
    PermanentContent = 8
    BadDeletionKey = 9
    DeletionKeyRequired = 10


class WithInputUlidAndHash(BaseModel):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None

class NotExistingContent(WithInputUlidAndHash):
    hasExisted: bool

class HashNotMatchingContent(WithInputUlidAndHash):
    inputHash: bytes  # none is not an option there
    expectedHash: bytes

class AlreadyUploadedContent(BaseModel):
    ulidUploaded: DefaultBaseType.ULID
    hashUploaded: bytes

class NotEnoughSpaceRemaining(BaseModel):
    requestedSize: int
    remainingSize: int

class OutOfSpaceConstraints(BaseModel):
    requestedSize: int
    maximumSize: int

class AlreadyUploadedChunk(BaseModel):
    chunkIndex: int

class BadChunkUploaded(BaseModel):
    chunkIndex: int
    expectedSize: int
    receivedSize: int

class PermanentContent(WithInputUlidAndHash):
    pass

class BadDeletionKey(WithInputUlidAndHash):
    pass

class DeletionKeyRequired(WithInputUlidAndHash):
    pass


#FilerExceptions = FilerCommon.register_serialization_child_like(Interaction, InteractionType.Exception, FilerExceptionType)
FilerExceptions = FilerInteractions.register_serialization_child(InteractionType.Exception, FilerExceptionType)

FilerExceptions.register_serialization_leaf(FilerExceptionType.NotExistingContent, NotExistingContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.HashNotMatchingContent, HashNotMatchingContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.AlreadyUploadedContent, AlreadyUploadedContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.NotEnoughSpaceRemaining, NotEnoughSpaceRemaining)
FilerExceptions.register_serialization_leaf(FilerExceptionType.OutOfSpaceConstraints, OutOfSpaceConstraints)
FilerExceptions.register_serialization_leaf(FilerExceptionType.AlreadyUploadedChunk, AlreadyUploadedChunk)
FilerExceptions.register_serialization_leaf(FilerExceptionType.BadChunkUploaded, BadChunkUploaded)
FilerExceptions.register_serialization_leaf(FilerExceptionType.BadDeletionKey, BadDeletionKey)
FilerExceptions.register_serialization_leaf(FilerExceptionType.DeletionKeyRequired, DeletionKeyRequired)
