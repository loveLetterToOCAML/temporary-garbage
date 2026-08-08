from authority.signature.authenticated_object import AuthenticatedObject
from filer.filer_common.registry_protocol import SimpleListQueryRequest, SimpleListQueryResponse
from baseimplems.datastreams.constrained import StreamConstraints, StreamInformation
from filer.filer_backend_with_registry.integrity_report import HashableWithBytesRepr
from baseimplems.datastreams.stream_event import StreamEvent

from typing import Protocol, TypeVar, Generic
from dataclasses import dataclass
from datetime import datetime

from pydantic import BaseModel


ServerFailureType = TypeVar('ServerFailureType')


class OutputStreamInformation(BaseModel):
    streamStatus: StreamEvent
    streamInformation: StreamInformation


C = TypeVar('C')  # upload / download / deletion intent context
H = TypeVar('H', bound=HashableWithBytesRepr)  # hash type
R = TypeVar('R')  # remote process type (external servers that can proceed the data)
I = TypeVar('I')  # identity type for ticket (unique id, ex: guid or ulid)
U = TypeVar('U')  # human identifier (ulid)
M = TypeVar('M')  # metadata type as stored within the registry

A = TypeVar('A')  # chosen computation algorithm type for proof of knowledge
P = TypeVar('P')  # proof of knowledge type
S = TypeVar('S')  # signature type that allows to verify proof of knowledge
O = TypeVar('O')  # abstract signed object


@dataclass(frozen=True, slots=True)
class BaseTicket(Generic[C, H, R, I]):
    uniqueId: I   # unique ID associated with the action or the transaction (upload, download, deletion)
    context: C    # context representing any metadata associated with current action / transaction (userid, date start, ...)
    hash: H       # hash type to locate the target of the action
    remoteProcessOn: R | None  # optional server location for processing the action instead of current system

@dataclass(frozen=True, slots=True)
class TransactionBaseTicket(BaseTicket[C, H, R, I]):
    emittedAt: datetime
    validUntil: datetime
    streamConstraints: StreamConstraints

@dataclass(frozen=True, slots=True)
class UploadTicket(TransactionBaseTicket[C, H, R, I, U]):
    expectedSize: int
    humanIdentifier: U | None

@dataclass(frozen=True, slots=True)
class DownloadTicket(TransactionBaseTicket[C, H, R, I]):
    pass

@dataclass(frozen=True, slots=True)
class DeleteContentTicket(BaseTicket[C, H, R, I]):
    pass


# for the complex workflow of knowledge proof to ensure requester of an already uploaded object knows the full object
@dataclass(frozen=True, slots=True)
class ProofOfKnowledgeRequestTicket(BaseTicket[A]):
    computationProofAlgorithm: A
    validUntil: datetime

@dataclass(frozen=True, slots=True)
class AuthenticatedProofOfKnowledgeRequestTicket(BaseTicket[C, H, R, I, A, P, S]):
    authenticatedRequest: AuthenticatedObject[ProofOfKnowledgeRequestTicket[C, H, R, I, A, P], S]
    computedProof: P | None

@dataclass(frozen=True, slots=True)
class ProofOfKnowledgeResponseTicket(BaseTicket[C, H, R, I, A]):
    computationProofAlgorithm: A
    succeeded: bool

@dataclass(frozen=True, slots=True)
class AuthenticatedProofOfKnowledgeResponseTicket(BaseTicket[C, H, R, I, A, S]):
    authenticatedResponse: AuthenticatedObject[ProofOfKnowledgeResponseTicket[C, H, R, I, A, P], S]


class EffectfulFilerServer(Protocol[H, ServerFailureType]):

    async def size_of_content_at_exn(self, hash: H) -> int | None:
        ...


    async def upload_start_at_exn(self, hash: H, total_size: int) -> UploadTicket[C, H, R, I, U] | AuthenticatedProofOfKnowledgeRequestTicket[C, H, R, I, A, P, S]:
        ...

    async def upload_chunk_at_exn(self, upload_ticket: UploadTicket[C, H, R, I, U], offset: int, data: bytes) -> int:
        ...

    async def upload_progress_at_exn(self, upload_ticket: UploadTicket[C, H, R, I, U]) -> OutputStreamInformation:
        ...

    async def upload_terminate_at_exn(self, upload_ticket: UploadTicket[C, H, R, I, U] | AuthenticatedProofOfKnowledgeResponseTicket[C, H, R, I, A, S]) -> BaseTicket[C, H, R, I, U] | bool:
        ...

    async def check_proof_of_knowledge_exn(self, proof: AuthenticatedProofOfKnowledgeRequestTicket[C, H, R, I, A, P, S]) -> AuthenticatedProofOfKnowledgeResponseTicket[C, H, R, I, A, S] | None:
        ...


    async def download_start_exn(self, hash: H) -> DownloadTicket[C, H, R, I]:
        ...

    async def download_chunk_from_exn(self, download_ticket: DownloadTicket[C, H, R, I], offset: int, size: int) -> bytes:
        ...

    async def download_progress_at_exn(self, download_ticket: DownloadTicket[C, H, R, I]) -> OutputStreamInformation:
        ...

    async def download_terminate_at_exn(self, download_ticket: DownloadTicket[C, H, R, I]) -> DownloadTicket[C, H, R, I] | bool:
        ...


    async def delete_resource_at_exn(self, deletion_ticket: DeleteContentTicket[C, H, R, I]) -> DeleteContentTicket[C, H, R, I] | bool:
        ...


    # below are all the protocol functions for FilerRegistry; there is a simple default implementation for these which calls
    # the same exact functions on the internal registry object
    # except the create and delete functions which are called when doing the effectful upload and delete above
    async def list_items_exn(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[M]:
        ...

    async def list_items_of_type_exn(self, item_type: type[M | H | str | int], request: SimpleListQueryRequest) -> SimpleListQueryResponse[M | H | str | int]:
        ...

    async def hash_for_ulid_exn(self, ulid: U) -> H | None:
        ...

    async def ulid_for_hash_exn(self, hash: H) -> U | None:
        ...

    async def check_hash_and_ulid_exn(self, hash: H, ulid: U) -> bool | None:  # convention: bool is if hash exists
        ...

    async def metadata_for_hash_exn(self, hash: H) -> M | bool | None:  # bool : True is convention for deleted elements
        ...

    async def old_metadata_for_hash_exn(self, hash: H) -> M | None:  # returns the metadata even if object is deleted
        ...


    # this is supposed to mix the BackendFailure and RegistryFailure
    def serialize_server_failure_exception(self, exn: Exception) -> ServerFailureType:
        ...
