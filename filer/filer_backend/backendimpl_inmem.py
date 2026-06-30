from pydantic import BaseModel

from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.base_exceptions import NotExistingContent, HashNotMatchingContent, AlreadyUploadedContent, \
    NotEnoughSpaceRemaining, OutOfSpaceConstraints, PermanentContent
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore

from contextlib import asynccontextmanager

from filer.base_types import GetContentSizeIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent, GetContentIntent, UploadContentIntent, DeleteContentIntent, UploadChunkIntent, UploadProgressIntent
from filer.filer_backend.backend_proto import FilerBackend

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


class FilerBackendInMem(FilerBackend[H]):

    def __init__(self, params):
        self._params = params
        self._files_per_hash: dict[H, bytes] = {}
        self._hashes_per_ulid: dict[H, str] = {}
        self._ulids_per_hash: dict[str, H] = {}

    async def get_content_hash_for_ulid(self, ulid: DefaultBaseType.ULID) -> H | None:
        if ulid in self._hashes_per_ulid:
            return self._hashes_per_ulid[ulid]

    async def get_content_ulid_for_hash(self, hash: H) -> DefaultBaseType.ULID | None:
        if hash in self._ulids_per_hash:
            return self._ulids_per_hash[hash]

    async def check_content_for_hash_and_ulid(self, hash: H, ulid: DefaultBaseType.ULID) -> bool | None:
        if hash in self._ulids_per_hash:
            return self._ulids_per_hash[hash] == ulid

    async def get_content_size(self, hash: H) -> int | None:
        if hash in self._files_per_hash:
            return len(self._files_per_hash[hash])

    async def get_content_start(self, hash: H):
        ...

    async def upload_content_start(self, expected_hash: bytes, wanted_size: int, expected_bytes_per_second: int):
        ...

    async def upload_content_chunk(self, offset: int, data: bytes):
        ...

    async def delete_content(self, hash: bytes):
        ...

    async def confirm_delete_content(self, hash: bytes, key: bytes):
        ...

    async def list_content(self):
        ...

    async def _internal_in_progress_check_and_clean(self):
        pass
