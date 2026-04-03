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



class UploadContentIntent(BaseModel):
    kind: Literal[FilerBackendIntentType.UploadContent] = FilerBackendIntentType.UploadContent
    totalSize: int
    dataHash: bytes
    requestedChunkSize: int

class UploadReadyToStart(BaseModel):
    kind: Literal[FilerBackendIntentType.UploadReady] = FilerBackendIntentType.UploadReady
    uploadTicket: bytes
    chunkSize: int


class UploadFinished(BaseModel):
    kind: Literal[FilerBackendIntentType.UploadFinished] = FilerBackendIntentType.UploadFinished
    ulidUploaded: ULID
    hashUploaded: bytes
    totalSizeUploaded: int

class UploadChunk(BaseModel):
    kind: Literal[FilerBackendIntentType.UploadContentChunk] = FilerBackendIntentType.UploadContentChunk
    uploadTicket: bytes
    data: bytes

class UploadProgress(BaseModel):
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


class GetContentPerHashIntent(PerHash):
    pass

class GetContentPerUlidIntent(PerUlid):
    pass

class GetContentIntent(BaseModel):
    kind: Literal[FilerBackendIntentType.GetContent] = FilerBackendIntentType.GetContent
    intent: GetContentPerHashIntent | GetContentPerUlidIntent

class GetContentSizePerHashIntent(PerHash):
    pass

class GetContentSizePerUlidIntent(PerUlid):
    pass

class GetContentSizeIntent(BaseModel):
    kind: Literal[FilerBackendIntentType.GetContentSize] = FilerBackendIntentType.GetContentSize
    intent: GetContentSizePerHashIntent | GetContentSizePerUlidIntent

class GetContentUlidForHashIntent(PerHash):
    kind: Literal[FilerBackendIntentType.GetContentUlidForHash] = FilerBackendIntentType.GetContentUlidForHash

class GetContentHashForUlidIntent(PerUlid):
    kind: Literal[FilerBackendIntentType.GetContentHashForUlid] = FilerBackendIntentType.GetContentHashForUlid

class CheckContentForHashAndUlidIntent(BaseModel):
    kind: Literal[FilerBackendIntentType.CheckContentForHashAndUlid] = FilerBackendIntentType.CheckContentForHashAndUlid
    hash: bytes
    ulid: ULID


class DeleteContentForHashIntent(PerHash):
    confirmDeletionKey: bytes | None = None

class DeleteContentForUlidIntent(PerUlid):
    confirmDeletionKey: bytes | None = None

class DeleteContentIntent(BaseModel):
    kind: Literal[FilerBackendIntentType.DeleteContent] = FilerBackendIntentType.DeleteContent
    intent: DeleteContentForHashIntent | DeleteContentForUlidIntent
