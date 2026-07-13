from filer.filer_backend.utils_exn import PydanticException, SerialException
from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.filer_type_registration import FilerInteractions
from basetypes.ae_interaction import InteractionType

from enum import Enum


class FilerExceptionType(Enum):
    NotExistingContent = 1
    NotExistingPlaceholderForUpload = 2
    HashNotMatchingContent = 3
    AlreadyUploadedContent = 4
    AlreadyUploadingContent = 5
    NotEnoughSpaceRemaining = 6
    OutOfSpaceConstraints = 7
    AlreadyUploadedChunk = 8
    BadChunkUploaded = 9
    PermanentContent = 10
    BadDeletionKey = 11
    DeletionKeyRequired = 12


class PydanticFilerException(PydanticException):
    pass

class WithInputUlidAndHash(PydanticFilerException):
    inputUlid: DefaultBaseType.ULID | None = None
    inputHash: bytes | None = None

class NotExistingContent(WithInputUlidAndHash):
    hasExisted: bool | None = None

class NotExistingPlaceholderForUpload(WithInputUlidAndHash):
    pass

class HashNotMatchingContent(WithInputUlidAndHash):
    inputHash: bytes  # none is not an option there
    expectedHash: bytes

class AlreadyUploadedContent(PydanticFilerException):
    existingUlid: DefaultBaseType.ULID | None
    hashAttempted: bytes

# TODO: remove this and act in consequence, because one could block any upload targeting a given hash with this
class AlreadyUploadingContent(PydanticFilerException):
    hashUploading: bytes

class NotEnoughSpaceRemaining(PydanticFilerException):
    requestedSize: int
    remainingSize: int

class OutOfSpaceConstraints(PydanticFilerException):
    requestedSize: int
    maximumSize: int

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
