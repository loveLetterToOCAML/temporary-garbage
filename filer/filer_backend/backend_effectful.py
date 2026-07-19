import os.path
from asyncio import Protocol
from dataclasses import field
from datetime import datetime
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import final, AsyncIterator

import anyio
from pydantic import BaseModel
from ulid import ULID

from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.base_exceptions import NotExistingContent, HashNotMatchingContent, AlreadyUploadedContent, \
    NotEnoughSpaceRemaining, OutOfSpaceConstraints, PermanentContent
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore, move_on_after, \
    Event, CapacityLimiter

from contextlib import asynccontextmanager

from filer.base_types import GetContentSizeIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent, GetContentIntent, UploadContentIntent, DeleteContentIntent, UploadChunkIntent, \
    UploadProgressIntent, PerHash, PerUlid
from filer.filer_common.registry_inmem import InMemRegistry, InMemRegistryInContext
from filer.filer_common.registry_protocol import Registry


UlidType = TypeVar('UlidType', bound=ReprEnforced)


FilerBackendIntent = GetContentSizeIntent | GetContentUlidForHashIntent | GetContentHashForUlidIntent | CheckContentForHashAndUlidIntent | \
    GetContentIntent | UploadContentIntent | DeleteContentIntent

FilerBackendResult = QueryResult[int, NotExistingContent] | \
                      QueryResult[DefaultBaseType.ULID, NotExistingContent] | \
                      QueryResult[bytes, NotExistingContent] | \
                      QueryResult[bool, NotExistingContent | HashNotMatchingContent] | \
                      QueryResult[GetContentTicket, NotExistingContent] | \
                      QueryResult[UploadContentTicket, AlreadyUploadedContent | NotEnoughSpaceRemaining | OutOfSpaceConstraints] | \
                      QueryResult[bool, NotExistingContent | PermanentContent]




class InMemFilerInternalState:

    def __init__(self, max_simultanous_uploads=0x1000, max_simultanous_downloads=0x1000):
        self._uploads_in_progress = {}
        self._downloads_in_progress = {}
        self._contents = {}

    def handle_upload_in_progress(self):
        pass

    def handle_download_in_progress(self):
        pass


class SimpleMetadata(BaseModel):
    dateBeginUpload: datetime
    dateEndUpload: datetime
    numberOfAccesses: int

class SimpleMetadataWithId(BaseModel[HashType, UlidType]):
    metadata: SimpleMetadata
    hash: HashType
    ulid: UlidType



class IntegrityReport(BaseModel[HashType, UlidType, ExternalResourceType]):
    unexpectedItems: Dict[ExternalResourceType, bool]   # bool is convention for is_deleted
    contentNotMatchingHashes: Dict[ExternalResourceType, bool]   # bool is convention for is_deleted
    contentMatchingHashes: List[SimpleMetadataWithId]
    contentUnknownMatchingHashes: List[SimpleMetadataWithId]


class PublicLevelType(Enum):
    PARTIAL_OBFUSCATED_RAM_AND_ENCRYPTED_SPLIT_STORAGE = 1
    OBFUSCATED_RAM_ENCRYPTED_STORAGE_ISOLATED = 2
    RAM_ENCRYPTED_STORAGE_ISOLATED = 3
    RAW_STORAGE_ISOLATED = 4
    RAW_STORAGE_SHARED = 5
    RAW_STORAGE_INTERNAL = 6
    ENCRYPTED_STORAGE_PUBLIC_NOT_EXPOSED = 7
    RAW_STORAGE_PUBLIC_NOT_EXPOSED = 8
    ENCRYPTED_STORAGE_PUBLIC = 9
    RAW_STORAGE_PUBLIC = 10

class BackendType:
    publicLevelType: PublicLevelType
    publicLevel: int  # 0 is the most public / easy access way, when higher it means more complex to find the content
    costPerByte: float
    costPerNetworkByte: float






class AsyncTimeController(AsyncContextManagerMixin):

    def __init__(self):
        ...

    async def _monitor_throughput(self):
        async with StatsForStreamProcessing() as (send_intent, receive_stats):
            while not self._operation_finished.is_set():
                with move_on_after(self._params.pollTime) as max_time:
                    try:
                        raw = await it.__anext__()
                    except StopAsyncIteration:
                        break

                if max_time.cancelled_caught:
                    logger.info("Cancelling task, max time elapsed")

    async def _process_loop(self):
        async with self._internal_receive_orders:
            async for order in self._internal_receive_orders:
                self._process_order(order)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        send_orders, self._internal_receive_orders = create_memory_object_stream()
        self._operation_finished = Event()
        async with (
            StatsForStreamProcessing() as (send_packets, send_intent, receive_stats),
            create_task_group() as tg
        ):
            tg.start_soon(self._process_loop, send_packets)
            tg.start_soon(self._monitor_throughput, receive_stats, )
            yield send_orders



class EasyProtocol(Protocol[LocatorType]):

    def locator_for_hash_exn(self, hash: HashType, *, is_placeholder: bool = False, ensure_exists: bool = False) -> \
            NotExistingContentForHash[HashType] | LocatorType:
        ...




class FilerBackend(AsyncContextManagerMixin):

    def __init__(self, params: FilerBackendInMemParameters, bytes_to_hash: Callable[[HashType], bytes], internal_registry: Registry[HashType, ULID, SimpleMetadata] | None = None):
        self._params = params
        self._internal_state = None
        self._bytes_to_hash = bytes_to_hash
        self._internal_registry = internal_registry or InMemRegistryInContext(
            hash_type=HashType, ulid_type=ULID, metadata_type=SimpleMetadata, keep_deleted_metadata=True
        )

    @final
    async def ensure_integrity(self, delete_bad: bool = False, compute_hash: bool = True) -> IntegrityReport[HashType, UlidType, ExternalResourceLocatorType]:
        unexpected = []
        good = {}
        unknown_hash = {}
        async for resource in list_resources_reorganize():
            hash = self.parse_hash_from_resource(resource)
            if not hash:
                unexpected.append(resource)
            if delete_bad and not hash:
                await self.delete_arbitrary_resource_exn(resource)
            if hash and compute_hash and not await self.check_integrity_for_exn(hash):
                pass
            if hash and not compute_hash:
                unknown_hash.add(hash)


    def _hash_from_intent(self, intent: PerUlid | PerHash) -> HashType:
        match intent.intent:
            case PerHash():
                return self._bytes_to_hash(intent.intent.hash)
            case _:
                hash = self._internal_registry.hash_for_ulid_exn(intent.intent.ulid)
                return self._bytes_to_hash(hash)

    async def _process_intent(self, intent: FilerBackendIntent):
        match intent:
            case GetContentSizeIntent():
                hash = self._hash_from_intent(intent.intent)
                return await self._internal_registry.size_for_hash(hash)
            case GetContentUlidForHashIntent():
                return await self._internal_registry.ulid_for_hash(self._bytes_to_hash(intent.hash))
            case GetContentHashForUlidIntent():
                return await self._internal_registry.hash_for_ulid_exn(intent.ulid)
            case CheckContentForHashAndUlidIntent():
                return await self._internal_registry.check_hash_and_ulid(self._bytes_to_hash(intent.hash), intent.ulid)
            case GetContentIntent():
                hash = self._hash_from_intent(intent.intent)
                return await self._internal_state.generate_new_download_ticket_for(hash)
            case UploadContentIntent():
                ticket = await self._internal_state.generate_new_upload_ticket_for(
                    self._bytes_to_hash(intent.hash), intent.totalSize, intent.requestedChunkSize
                )
                if success_of(ticket):
                    self._monitor_for_upload_success(ticket)
                return ticket
            case DeleteContentIntent():
                hash: bytes = self._hash_from_intent(intent.intent)
                hashed: HashType = self._bytes_to_hash(hash)
                deleted_result = await self._internal_state.delete_content(hashed)
                if success_of(deleted_result):
                    self._internal_registry.delete_item(hashed)
                return deleted_result
            case _:
                raise NotImplementedError

    async def _process_intent_and_send_result(self, intent):
        response = await self._process_intent(intent)
        await self._internal_result_stream.send(response)

    async def _process_intents(self, task_group):
        if not self._receive_intent:
            raise NotInAsyncContextManager('_process_intents', 'FilerBackend')

        async with (
            self._receive_intent,
            self._internal_result_stream
        ):
            async for intent in self._receive_intent:
                async with self._semaphore:
                    task_group.start_soon(self._process_intent_and_send_result, intent)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._internal_result_stream, external_receive_results = create_memory_object_stream[FilerBackendResult](0x40)
        external_send_intent, self._receive_intent = create_memory_object_stream[FilerBackendIntent](0x40)
        self._semaphore = CapacityLimiter(0x10)

        async with (
            create_task_group() as task_group,
        ):
            task_group.start_soon(self._process_intents, task_group)
            yield external_send_intent


if __name__ == '__main__':
    import anyio

    async def main():
        async with FilerBackend() as send_intent:
            pass

    anyio.run(main)
