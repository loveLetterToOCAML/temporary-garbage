from basetypes.implementation.basetypes_match import DefaultBaseType

from pydantic import BaseModel

from typing import Literal, List
from enum import Enum



class AddressingType(Enum):
    HASH = 1
    ULID = 2

class PerHash(BaseModel):
    addressingType: Literal[AddressingType.HASH] = AddressingType.HASH
    hash: bytes

class PerUlid(BaseModel):
    addressingType: Literal[AddressingType.ULID] = AddressingType.ULID
    ulid: DefaultBaseType.ULID


class CommonFilerIntentType(Enum):
    GetContent = 1
    GetContentSize = 2
    GetContentUlidForHash = 3
    GetContentHashForUlid = 4
    CheckContentForHashAndUlid = 5

    DeleteContent = 20
    UploadContent = 21
    UploadContentChunk = 22
    UploadReady = 23
    UploadProgress = 24

    CheckIntegrity = 30
    CheckBackend = 31

    UploadFinished = 31



from pydantic import BaseModel
from typing import List


class FileContent(BaseModel):
    hash: bytes
    ulid: DefaultBaseType.ULID

class FileMetadata(BaseModel):
    pass

class File(BaseModel):
    location: List[str]
    name: str



class UploadContentIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.UploadContent] = CommonFilerIntentType.UploadContent
    totalSize: int
    dataHash: bytes
    requestedChunkSize: int

class UploadReadyToStart(BaseModel):
    kind: Literal[CommonFilerIntentType.UploadReady] = CommonFilerIntentType.UploadReady
    uploadTicket: bytes
    chunkSize: int
    expectedSize: int
    expectedMaxDelay: DefaultBaseType.TIMEDELTA


class UploadFinished(BaseModel):
    kind: Literal[CommonFilerIntentType.UploadFinished] = CommonFilerIntentType.UploadFinished
    ulidUploaded: DefaultBaseType.ULID
    hashUploaded: bytes
    totalSizeUploaded: int

class UploadChunkIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.UploadContentChunk] = CommonFilerIntentType.UploadContentChunk
    uploadTicket: bytes
    data: bytes

class UploadProgressIntent(BaseModel):
    uploadTicket: bytes
    precise: bool = False

class AllUploadsProgress(BaseModel):  # more like in introspection intent
    pass

class UploadProgressResult(BaseModel):
    uploadTicket: bytes
    missingChunks: int | List[int]
    uploadedChunks: int | List[int]
    expectedChunks: int
    remainingTimeToUpload: float


class GetContentIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.GetContent] = CommonFilerIntentType.GetContent
    intent: PerHash | PerUlid

class GetContentSizeIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.GetContentSize] = CommonFilerIntentType.GetContentSize
    intent: PerHash | PerUlid

class GetContentUlidForHashIntent(PerHash):
    kind: Literal[CommonFilerIntentType.GetContentUlidForHash] = CommonFilerIntentType.GetContentUlidForHash

class GetContentHashForUlidIntent(PerUlid):
    kind: Literal[CommonFilerIntentType.GetContentHashForUlid] = CommonFilerIntentType.GetContentHashForUlid

class CheckContentForHashAndUlidIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.CheckContentForHashAndUlid] = CommonFilerIntentType.CheckContentForHashAndUlid
    hash: bytes
    ulid: DefaultBaseType.ULID


class DeleteContentForHashIntent(PerHash):
    confirmDeletionKey: bytes | None = None

class DeleteContentForUlidIntent(PerUlid):
    confirmDeletionKey: bytes | None = None

class DeleteContentIntent(BaseModel):
    kind: Literal[CommonFilerIntentType.DeleteContent] = CommonFilerIntentType.DeleteContent
    intent: DeleteContentForHashIntent | DeleteContentForUlidIntent
