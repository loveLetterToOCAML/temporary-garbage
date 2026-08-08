from contextlib import asynccontextmanager

from attr.exceptions import NotCallableError

from baseimplems.date_utils import utc_now
from basetypes.implementation.dataformat.hashed import Hashed
from filer.base_exceptions import NotExistingContent, NotExistingPlaceholder, FilerSerialException
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
    ProofOfKnowledgeRequestTicket, DownloadTicket, DeleteContentTicket
from filer.filter_registry.registry import FilerRegistry

ServerFailureType = TypeVar('ServerFailureType')


class OutputStreamInformation(BaseModel):
    streamStatus: StreamEvent
    streamInformation: StreamInformation


ContextType = TypeVar('ContextType')  # upload context
H = TypeVar('H', bound=HashableWithBytesRepr)  # hash type
R = TypeVar('R')  # remote process type (external servers that can proceed the data)
I = TypeVar('I')  # identity type for ticket (unique id, ex: guid or ulid)
U = TypeVar('U')  # human identifier (ulid)
M = TypeVar('M')  # metadata type as stored within the registry

A = TypeVar('A')  # chosen computation algorithm type for proof of knowledge
P = TypeVar('P')  # proof of knowledge type
S = TypeVar('S')  # signature type that allows to verify proof of knowledge
O = TypeVar('O')  # abstract signed object



class EffectfulFilerServerOrchestrator(

):

    def __init__(self, server_params: KnownServerParameters):
        self._params = server_params

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._server = FilerServerFor(self._params)
        self._current_placeholder_index = 0
        self._unique_operations = {}
        async with self._server as report:
            print(report)
            yield


    async def size_of_content_at_exn(self, hash: Hashed) -> int | None:
        return await self._server.size_of_content_at_exn(hash)

    async def upload_start_at_exn(self, hash: H, total_size: int) -> UploadTicket[ContextType, H, R, I, U] | AuthenticatedProofOfKnowledgeRequestTicket[C, H, R, I, A, P, S]:
        raise NotCallableError  # TODO: change the exception as it does not mean this

    async def upload_chunk_at_exn(self, upload_ticket: UploadTicket[ContextType, H, R, I, U], offset: int, data: bytes) -> int:
        ticket_id = upload_ticket.uniqueId
        if ticket_id in self._terminated_operations:
            raise FilerSerialException(
                TerminatedOperation(
                    operationId=upload_ticket.uniqueId,
                    operationType='upload'
                )
            )
        # TODO: check ticket constraints, merge constraints with filer server constraints (take the most constrained)
        if not ticket_id in self._unique_operations:
            ph_index = self._current_placeholder_index
            self._current_placeholder_index += 1
            await self._server.prepare_placeholder_at_exn(hash, ph_index, upload_ticket.expectedSize)
            self._unique_operations[ticket_id] = ph_index
            # TODO: start upload monitoring
        # TODO: await self._server.new_item_exn(hash, UploadInProgressMetadata, sz)  # but it does not fit into RO write once model and it could be misleading if one attempts to download unfinished content
        return await self._server.upload_chunk_at_exn(upload_ticket.hash, self._unique_operations[ticket_id], offset, data)


    async def _ensure_existing(self, ticket: UploadTicket[ContextType, H, R, I, U] | DownloadTicket[C, H, R, I] | DeleteContentTicket[C, H, R, I]):
        existing_operation = self._unique_operations.get(ticket.uniqueId)
        if not existing_operation and isinstance(ticket, UploadTicket):
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=ticket.hash,
                    placeholderIndex=-1
                )
            )
        raise FilerSerialException(
            NotExistingContent(
                inputHash=ticket.hash,
                hasExisted=await self._server.metadata_for_hash_exn(ticket.hash)
            )
        )

    async def upload_progress_at_exn(self, upload_ticket: UploadTicket[ContextType, H, R, I, U]) -> OutputStreamInformation:
        await self._ensure_existing(upload_ticket)
        placeholder_index = self._unique_operations[upload_ticket.uniqueId]
        return await self._server.current_progress_for(placeholder_index)

    async def upload_terminate_at_exn(self, upload_ticket: UploadTicket[ContextType, H, R, I, U] | AuthenticatedProofOfKnowledgeResponseTicket[ContextType, H, R, I, A, S]) -> BaseTicket[ContextType, H, R, I, U] | bool:
        if isinstance(upload_ticket, UploadTicket):
            await self._ensure_existing(upload_ticket)
            await self._server.upload_terminate_at_exn(upload_ticket.hash, self._unique_operations[upload_ticket.uniqueId])
            return True
        # otherwise it's the case where the content should be referenced if proof of knowledge is validated
        raise NotCallableError  # TODO: change to good exception

    async def check_proof_of_knowledge_exn(self, proof: AuthenticatedProofOfKnowledgeRequestTicket[C, H, R, I, A, P, S]) -> AuthenticatedProofOfKnowledgeResponseTicket[C, H, R, I, A, S] | None:
        if proof.computedProof is None:
            return
        async with proof.authenticatedRequest.signature:
            pass
        hash = proof.hash
        with hash.compute_new() as h:
            size = await self.size_for_hash_exn(hash)
            for offset in range(0, size, chunk_size):
                h.update(await self.download_chunk_for_hash_exn(hash, offset, chunk_size))
            return h.is_same(hash.hash)


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
