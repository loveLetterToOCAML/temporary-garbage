from pydantic import BaseModel

from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.base_exceptions import NotExistingContent, HashNotMatchingContent, AlreadyUploadedContent, \
    NotEnoughSpaceRemaining, OutOfSpaceConstraints, PermanentContent
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore

from contextlib import asynccontextmanager

from filer.base_types import GetContentSizeIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent, GetContentIntent, UploadContentIntent, DeleteContentIntent, UploadChunkIntent, UploadProgressIntent


FilerBackendIntent = GetContentSizeIntent | GetContentUlidForHashIntent | GetContentHashForUlidIntent | CheckContentForHashAndUlidIntent | \
    GetContentIntent | UploadContentIntent | DeleteContentIntent

FilerBackendResult = QueryResult[int, NotExistingContent] | \
                      QueryResult[DefaultBaseType.ULID, NotExistingContent] | \
                      QueryResult[bytes, NotExistingContent] | \
                      QueryResult[bool, NotExistingContent | HashNotMatchingContent] | \
                      QueryResult[GetContentTicket, NotExistingContent] | \
                      QueryResult[UploadContentTicket, AlreadyUploadedContent | NotEnoughSpaceRemaining | OutOfSpaceConstraints] | \
                      QueryResult[bool, NotExistingContent | PermanentContent]


class FilerBackendInMemConfig(BaseModel):
    allowedMemory: int = 0x40000000
    streamConstraints: StreamConstraints = StreamConstraints(
        minBytesPerSecond=1000000
    )


class FilerBackendInMemParameters(ExecutionSystemParameters):
    backendConfig: FilerBackendInMemConfig

