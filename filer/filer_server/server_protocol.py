from datetime import datetime

from pydantic import BaseModel

from typing import Protocol, TypeVar

from baseimplems.datastreams.constrained import StreamConstraints, StreamInformation
from baseimplems.datastreams.stream_event import StreamEvent
from filer.filer_server.integrity_report import HashableWithBytesRepr

ServerFailureType = TypeVar('ServerFailureType')
HashType = TypeVar('HashType', bound=HashableWithBytesRepr)  # just for the compute_new context manager


class TicketFromRegistry(BaseModel):
    expectedSize: int
    emittedAt: datetime
    validUntil: datetime
    streamConstraints: StreamConstraints

class OutputStreamInformation(BaseModel):
    streamStatus: StreamEvent
    streamInformation: StreamInformation

class SignedTicketFromRegistry(BaseModel):
    ticket: TicketFromRegistry
    signature: bytes


class EffectfulFilerServer(Protocol[HashType, ServerFailureType]):

    async def size_of_content_at_exn(self, hash: HashType) -> int | None:
        ...

    async def upload_chunk_at_exn(self, upload_ticket: UploadTicketType[HashType], offset: int, data: bytes) -> int:
        ...

    async def upload_progress_at_exn(self, upload_ticket: UploadTicketType[HashType]) -> UploadStreamInformationType:
        ...

    async def upload_terminate_at_exn(self, upload_ticket: UploadTicketType[HashType]):
        ...

    async def download_chunk_from_exn(self, download_ticket: DownloadTicketType[HashType], offset: int, size: int) -> bytes:
        ...

    async def download_progress_at_exn(self, download_ticket: DownloadTicketType[HashType]) -> DownloadStreamInformationType:
        ...

    async def delete_resource_at_exn(self, deletion_ticket: DeletionTicketType[HashType]) -> bool | None:
        ...


    async def list_items(self, request: ListQueryType) -> ListQueryResponseType:
        ...

    async def list_items_of_type(self, item_type: type[HashType | str | MetadataType | int | Any], request: ListQueryType) -> ListQueryResponseType[HashType | str | MetadataType | int | Any]:
        ...

    async def hash_for_ulid_exn(self, ulid: UlidType) -> HashType | None:
        ...

    async def ulid_for_hash_exn(self, hash: HashType) -> UlidType | None:
        ...

    async def check_hash_and_ulid_exn(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        ...

    async def metadata_for_hash_exn(self, hash: HashType) -> MetadataType | bool | None:  # bool : True is convention for deleted elements
        ...

    async def old_metadata_for_hash_exn(self, hash: HashType) -> MetadataType | None:  # returns the metadata even if object is deleted
        ...


    def serialize_server_failure_exception(self, exn: Exception) -> ServerFailureType:
        ...
