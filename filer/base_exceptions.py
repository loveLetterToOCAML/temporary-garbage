from basetypes.ae_interaction import Interaction, InteractionType
from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.filer_type_registration import FilerCommon

from pydantic import BaseModel

from enum import Enum


class FilerExceptionType(Enum):
    NotExistingContent = 1
    HashNotMatchingContent = 2
    AlreadyUploadedContent = 3
    NotEnoughSpaceRemaining = 4
    AlreadyUploadedChunk = 5
    BadChunkUploaded = 6
    BadDeletionKey = 7
    DeletionKeyRequired = 8


class NotExistingContent(BaseModel):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None
    hasExisted: bool

class HashNotMatchingContent(BaseModel):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes
    expectedHash: bytes

class AlreadyUploadedContent(BaseModel):
    ulidUploaded: DefaultBaseType.ULID
    hashUploaded: bytes

class NotEnoughSpaceRemaining(BaseModel):
    requestedSize: int
    remainingSize: int

class AlreadyUploadedChunk(BaseModel):
    chunkIndex: int

class BadChunkUploaded(BaseModel):
    chunkIndex: int
    expectedSize: int
    receivedSize: int

class BadDeletionKey(BaseModel):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None

class DeletionKeyRequired(BaseModel):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None


FilerExceptions = FilerCommon.register_serialization_child_like(Interaction, InteractionType.Exception, FilerExceptionType)

FilerExceptions.register_serialization_leaf(FilerExceptionType.NotExistingContent, NotExistingContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.HashNotMatchingContent, HashNotMatchingContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.AlreadyUploadedContent, AlreadyUploadedContent)
FilerExceptions.register_serialization_leaf(FilerExceptionType.NotEnoughSpaceRemaining, NotEnoughSpaceRemaining)
FilerExceptions.register_serialization_leaf(FilerExceptionType.AlreadyUploadedChunk, AlreadyUploadedChunk)
FilerExceptions.register_serialization_leaf(FilerExceptionType.BadChunkUploaded, BadChunkUploaded)
FilerExceptions.register_serialization_leaf(FilerExceptionType.BadDeletionKey, BadDeletionKey)
FilerExceptions.register_serialization_leaf(FilerExceptionType.DeletionKeyRequired, DeletionKeyRequired)
