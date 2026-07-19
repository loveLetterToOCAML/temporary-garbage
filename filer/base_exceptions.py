from filer.filer_backend.utils_exn import PydanticException, SerialException
from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.filer_type_registration import FilerInteractions
from basetypes.ae_interaction import InteractionType

from pydantic import BaseModel

from typing import TypeVar, Generic
from enum import Enum


class FilerExceptionType(Enum):
    NotExistingContent = 1
    NotExistingPlaceholderForUpload = 2
    HashNotMatchingContent = 3
    AlreadyUploadedContent = 4
    AlreadyUploadingContent = 5
    NotEnoughSpaceRemaining = 6
    OutOfSpaceConstraints = 7
    OutOfConstraints = 8
    AlreadyUploadedChunk = 9
    BadChunkUploaded = 10
    PermanentContent = 11
    BadDeletionKey = 12
    DeletionKeyRequired = 13


class PydanticFilerException(PydanticException):
    pass

class MultiplePydanticFilerException(PydanticFilerException):
    exceptions: list[PydanticFilerException]

class WithInputUlidAndHash(PydanticFilerException):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None

class NotExistingContent(WithInputUlidAndHash):
    hasExisted: bool | None = None

class NotExistingPlaceholderForUpload(WithInputUlidAndHash):
    placeholderIndex: int

class HashNotMatchingContent(WithInputUlidAndHash):
    inputHash: bytes  # none is not an option there
    expectedHash: bytes

class AlreadyUploadedContent(PydanticFilerException):
    existingUlid: DefaultBaseType.ULID | None
    hashAttempted: bytes

# TODO: remove this and act in consequence, because one could block any upload targeting a given hash with this
class AlreadyUploadingContent(PydanticFilerException):
    hashUploading: bytes
    placeholderIndex: int

class NotEnoughSpaceRemaining(PydanticFilerException):
    requestedSize: int
    remainingSize: int

class OutOfSpaceConstraints(PydanticFilerException):
    requestedSize: int
    maximumSize: int

class FilerConstraintType(Enum):
    NO_UPLOAD = 1
    NO_DOWNLOAD = 2
    NO_DELETION = 3

    MIN_TOTAL_SIZE = 10
    MAX_TOTAL_SIZE = 11
    MIN_CHUNK_SIZE = 12
    MAX_CHUNK_SIZE = 13
    FIXED_CHUNK_SIZE_EXPECTED = 14

    MAX_ELAPSED_DELAY_FOR_UPLOAD = 20
    MAX_ELAPSED_DELAY_FOR_NEXT_CHUNK = 21
    INSUFFICIENT_THROUGHPUT = 22

    TOO_MUCH_PARALLEL_UPLOADS = 30
    TOO_MUCH_PARALLEL_DOWNLOADS = 31

class PredicateType(Enum):
    ALWAYS_TRUE = 1
    ALWAYS_FALSE = 2
    EQUALS = 3
    STRICT_INFERIOR = 4
    INFERIOR = 5
    STRICT_SUPERIOR = 6
    SUPERIOR = 7

T = TypeVar('T')

class ExpectedAgainstReality(BaseModel, Generic[T]):
    ExpectationType: PredicateType
    referenceValue: T

class OutOfConstraints(PydanticFilerException):
    failedConstraint: FilerConstraintType
    details: ExpectedAgainstReality | None = None

class AlreadyUploadedChunk(PydanticFilerException):
    chunkIndex: int

class BadChunkUploaded(PydanticFilerException):
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


class FilerSerialException(SerialException):

    def __init__(self, serialized: PydanticFilerException):
        super().__init__(serialized, 'FilerException::')


if __name__ == '__main__':
    try:
        raise FilerSerialException(
            NotExistingContent(
                hasExisted=True,
                inputHash=b'x'
            )
        )
    except Exception as e:
        print(e)

    try:
        raise FilerSerialException(
            OutOfSpaceConstraints(
                requestedSize=13,
                maximumSize=14
            )
        )
    except Exception as e:
        print(e)

    try:
        raise FilerSerialException(
            NotExistingContent(
                hasExisted=False,
                inputHash=b'b'
            )
        )
    except Exception as e:
        print(e)
        raise
