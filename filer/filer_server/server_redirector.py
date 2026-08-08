from contextlib import asynccontextmanager

from baseimplems.date_utils import utc_now
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend_with_registry.server_factory import FilerServerFor
from filer.filer_common.registry_factory import KnownFilerRegistryParameters
from filer.filer_common.registry_protocol import SimpleListQueryRequest, SimpleListQueryResponse, Registry
from baseimplems.datastreams.constrained import StreamConstraints, StreamInformation
from filer.filer_backend_with_registry.integrity_report import HashableWithBytesRepr
from baseimplems.datastreams.stream_event import StreamEvent

from typing import Protocol, TypeVar, Generic
from dataclasses import dataclass
from datetime import datetime, timedelta

from pydantic import BaseModel

from filer.filer_server.server_protocol import UploadTicket, AuthenticatedProofOfKnowledgeRequestTicket, \
    ProofOfKnowledgeRequestTicket
from filer.filter_registry.registry import FilerRegistry

ServerFailureType = TypeVar('ServerFailureType')


class OutputStreamInformation(BaseModel):
    streamStatus: StreamEvent
    streamInformation: StreamInformation


UploadContextType = TypeVar('ContextType')  # upload context
H = TypeVar('H', bound=HashableWithBytesRepr)  # hash type
R = TypeVar('R')  # remote process type (external servers that can proceed the data)
I = TypeVar('I')  # identity type for ticket (unique id, ex: guid or ulid)
U = TypeVar('U')  # human identifier (ulid)
M = TypeVar('M')  # metadata type as stored within the registry

A = TypeVar('A')  # chosen computation algorithm type for proof of knowledge
P = TypeVar('P')  # proof of knowledge type
S = TypeVar('S')  # signature type that allows to verify proof of knowledge
O = TypeVar('O')  # abstract signed object



# the server implementation that serves as a main registry, only performing light query operations, crafting upload
# download or delete tickets for the actual filer servers (with backends) to process these
class EffectfulServerRedirector(
    EffectfulFilerServer[UploadContextType, Hashed, RemoteComputationIntent, ULID, ULID, MetadataType],
    DefaultFilerRegistryProxy[]
):

    def __init__(self, server_params: KnownServerParameters):
        self._params = server_params

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._server = FilerServerFor(self._params)
        async with self._server as report:
            print(report)
            yield


    async def size_of_content_at_exn(self, hash: Hashed) -> int | None:
        return await self._server.size_of_content_at_exn(hash)


    def _craft_base_ticket_arguments(self, hash: Hashed, context_type: UploadContextType, process_on: RemoteComputationIntent | None):
        return {
            'uniqueId': U(),
            'context': context_type,
            'hash': hash,
            'remoteProcessOn': process_on
        }

    def _craft_transaction_base_ticket_arguments(self, hash: Hashed, context_type: UploadContextType, process_on: RemoteComputationIntent | None):
        now = utc_now()
        return {
            **self._craft_base_ticket_arguments(hash, context_type, process_on),
            'emittedAt': now,
            'validUntil': now + timedelta(),
            'streamConstraints': self._params.streamConstraints
        }

    @dataclass(frozen=True, slots=True)
    class UploadTicket(TransactionBaseTicket[C, H, R, I, U]):
        expectedSize: int
        humanIdentifier: U | None

    async def upload_start_at_exn(self, hash: H, total_size: int) -> UploadTicket[UploadContextType, H, R, I, U] | AuthenticatedProofOfKnowledgeRequestTicket[UploadContextType, H, R, I, A]:
        sz = await self.size_of_content_at_exn(hash)
        if sz and sz != total_size:
            raise
        if sz:  # content already existing, will ask proof of knowledge before creating the actual content
            return AuthenticatedProofOfKnowledgeRequestTicket[UploadContextType, H, R, I, A](
                authenticatedRequest=await current_identity.sign_object(
                    ProofOfKnowledgeRequestTicket[A](
                        computationProofAlgorithm=A(),
                        validUntil=datetime.now() + self._params.proofRequestValidityDelay
                    )
                ),
                computedProof=None,
            )
        ph_index = self._current_placeholder_index
        self._current_placeholder_index += 1
        await self._server.prepare_placeholder_at_exn(hash, ph_index, total_size)
        await self._server.new_item_exn(hash, UploadInProgressMetadata, sz)
        return UploadTicket[UploadContextType, Hashed, RemoteComputationIntent, ULID, ULID](
            **self._craft_transaction_base_ticket_arguments(hash, ),
            expectedSize=total_size,
            humanIdentifier=created_ulid,
        )


    async def upload_chunk_at_exn(self, upload_ticket: UploadTicket[UploadContextType, H, R, I, U], offset: int, data: bytes) -> int:
        ...

    async def upload_progress_at_exn(self, upload_ticket: UploadTicket[UploadContextType, H, R, I, U]) -> OutputStreamInformation:
        ...

    async def upload_terminate_at_exn(self, upload_ticket: UploadTicket[UploadContextType, H, R, I, U] | AuthenticatedProofOfKnowledgeResponseTicket[UploadContextType, H, R, I, A, S]) -> BaseTicket[UploadContextType, H, R, I, U] | bool:
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



    # this is supposed to mix the BackendFailure and RegistryFailure
    def serialize_server_failure_exception(self, exn: Exception) -> ServerFailureType:
        ...


class DefaultFilerRegistryProxy(RegistryInContext[Hashed, ULID, MetadataType]):

    def __init__(self, params: KnownFilerRegistryParameters):
        super().__init__()
        self._params = params

    async def list_items_exn(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[M]:
        return await self._registry.list_items_exn(request)

    async def list_items_of_type_exn(self, item_type: type[M | H | str | int], request: SimpleListQueryRequest) -> SimpleListQueryResponse[M | H | str | int]:
        return await self._registry.list_items_of_type_exn(item_type, request)

    async def hash_for_ulid_exn(self, ulid: U) -> H | None:
        return await self._registry.hash_for_ulid_exn(ulid)

    async def ulid_for_hash_exn(self, hash: H) -> U | None:
        return await self._registry.ulid_for_hash_exn(hash)

    async def check_hash_and_ulid_exn(self, hash: H, ulid: U) -> bool | None:  # convention: bool is if hash exists
        return await self._registry.check_hash_and_ulid_exn(hash, ulid)

    async def metadata_for_hash_exn(self, hash: H) -> M | bool | None:  # bool : True is convention for deleted elements
        return await self._registry.metadata_for_hash_exn(hash)

    async def old_metadata_for_hash_exn(self, hash: H) -> M | None:  # returns the metadata even if object is deleted
        return await self._registry.old_metadata_for_hash_exn(hash)
