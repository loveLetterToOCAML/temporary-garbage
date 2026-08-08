from contextlib import asynccontextmanager
from typing import List, TypeVar, AsyncIterator

from anyio import create_task_group, CapacityLimiter, create_memory_object_stream, AsyncContextManagerMixin
from pydantic import BaseModel

from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.implementation.dataformat.hashed import Hashed
from basetypes.implementation.generics_match import DefaultGenericType
from filer.base_types import UploadContentIntent, UploadChunkIntent, UploadFinished, DeleteContentIntent, \
    GetContentSizeIntent, GetContentIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent
from filer.filer_backend.backend_failure import BackendFailure
from filer.filer_backend.backend_protocol import EffectfulFilerBackend, EffectfulBackend, BackendFailureType


class RemoteBackendInContextParameters(BaseModel):
    pass

ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')

class EffectfulRemoteFilerBackend(
    EffectfulFilerBackend[Hashed, ExternalResourceLocatorType, BackendFailure],
    EffectfulBackend[Hashed, BackendFailure],
    AsyncContextManagerMixin
):
    def __init__(self, params: RemoteBackendInContextParameters):
        self._params = params
        self._constraints = params.constraintParameters

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        async with create_task_group() as self._current_task_group:
            yield

    @property
    def _effectful_backend(self) -> EffectfulBackend[ExternalResourceLocatorType, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: ExternalResourceLocatorType) -> Hashed | None:
        return self._internal.hash_from_resource_locator(locator)

    def resource_locator_from_hash(self, hash: Hashed) -> ExternalResourceLocatorType:
        return self._internal.resource_locator_from_hash(hash)



class ArgumentToSerialAttribute(BaseModel):
    argumentName: str
    pydanticAttributeName: str
    pydanticType: DefaultBaseType.TYPE | None  # if none, the generic index below must be defined
    genericTypeIndex: str | None = None  # if defined, means the type is not yet fixed, and must be on instance creation


class ProtocolMethodAttributesMatch(BaseModel):
    methodName: str
    relatedPydanticModel: BaseModel
    returnType: DefaultBaseType.TYPE | None
    canRaiseException: bool = True
    argumentsToAttributes: List[ArgumentToSerialAttribute]


if __name__ == '__main__':

    locator_argument = ArgumentToSerialAttribute(
        argumentName='locator',
        pydanticAttributeName='hash',
        pydanticType=None,
        genericTypeIndex='ExternalResourceLocatorType'
    )

    placeholder_index_argument = ArgumentToSerialAttribute(
        argumentName='placeholder_index',
        pydanticAttributeName='placeholderIndex',
        pydanticType=int | None
    )

    total_size_argument = ArgumentToSerialAttribute(
        argumentName='total_size',
        pydanticAttributeName='totalSize',
        pydanticType=int,
    )

    offset_argument = ArgumentToSerialAttribute(
        argumentName='offset',
        pydanticAttributeName='offset',
        pydanticType=int,
    )

    data_argument = ArgumentToSerialAttribute(
        argumentName='data',
        pydanticAttributeName='offset',
        pydanticType=bytes,
    )

    size_argument = ArgumentToSerialAttribute(
        argumentName='size',
        pydanticAttributeName='size',
        pydanticType=int,
    )

    match_for_protocol = [
        ProtocolMethodAttributesMatch(
            methodName='size_of_content_at_exn',
            relatedPydanticModel=GetContentSizeIntent,
            returnType=int,
            argumentsToAttributes=[locator_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='prepare_placeholder_at_exn',
            relatedPydanticModel=PreparePlaceholderAtIntent,
            returnType=None,
            argumentsToAttributes=[locator_argument, placeholder_index_argument, total_size_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='upload_chunk_at_exn',
            relatedPydanticModel=UploadChunkIntent,
            returnType=int,
            argumentsToAttributes=[locator_argument, placeholder_index_argument, offset_argument, data_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='upload_terminate_at_exn',
            relatedPydanticModel=FinishUploadIntent,
            returnType=None,
            argumentsToAttributes=[locator_argument, placeholder_index_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='download_chunk_from_exn',
            relatedPydanticModel=DownloadChunkIntent,
            returnType=bytes,
            argumentsToAttributes=[locator_argument, offset_argument, size_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='delete_resource_at_exn',
            relatedPydanticModel=DeleteContentIntent,
            returnType=None,
            argumentsToAttributes=[locator_argument, placeholder_index_argument]
        ),
        ProtocolMethodAttributesMatch(
            methodName='_list_resources_reorganize_exn',
            relatedPydanticModel=ListResourcesIntent,
            returnType=None,  # don't know yet how to handle this case of returning the generic type
            argumentsToAttributes=[]
        ),
    ]

    auto_construct_network_client_from_protocol()
    auto_construct_network_server()


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')


class Client:

    def __init__(self, client_params):
        self._params = client_params

    # all methods below should be automatically populated by auto_construct_network_client_from_protocol
    async def size_of_content_at_exn(self, locator: ExternalResourceLocatorType) -> int:
        return await self._remote_peer.query(GetContentSizeIntent(intent=PerHash(hash=bytes(locator))))

    async def prepare_placeholder_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, total_size: int):
        return await self._remote_peer.send()

    async def upload_chunk_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, offset: int, data: bytes) -> int:
        return await self._remote_peer.send()

    async def upload_terminate_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int):
        return await self._remote_peer.send()

    async def download_chunk_from_exn(self, locator: ExternalResourceLocatorType, offset: int, size: int) -> bytes:
        return await self._remote_peer.send()

    async def delete_resource_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int = -1):
        return await self._remote_peer.send()

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
        return await self._remote_peer.send()

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailureType:
        return

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        async with (
            remote_peer_for() as self._remote_peer
        ):
            yield intent_queue_send


FilerBackendIntent = GetContentSizeIntent | GetContentIntent | UploadContentIntent | DeleteContentIntent

class Server:

    def __init__(self, server_params):
        self._params = server_params

    # this should be automatically constructed by helper
    async def _process_once(self, intent: FilerBackendIntent):
        match intent:
            case GetContentSizeIntent():
                return await self._internal.size_for_hash(intent.hash)
            case GetContentIntent():
                return
            # ...
            case _:
                return None

    # below is systematic server processing
    async def _process_intent(self, intent: FilerBackendIntent):
        async with self._capacity_limiter:
            result = await self._process_once(intent)
            if not result:
                await self._send_output_processed.send(UnkonwnIntent)
                return
            self._send_output_processed(result)

    async def _process_intents(self):
        async for intent in self._intent_queue_receive():
            self._process_intents_task.start_soon(self._process_intent, intent)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._capacity_limiter = CapacityLimiter(self._params.parallel_tasks)
        intent_queue_send, self._intent_queue_receive = create_memory_object_stream[FilerBackendIntent]()
        self._send_output_processed, receive_output_processed = create_memory_object_stream[FilerObject]()
        self._internal = FilerBackendFor(self._params.backend)
        async with (
            intent_queue_send,
            receive_output_processed,
            create_task_group() as self._process_intents_task,
        ):
            self._process_intents_task.start_soon(self._process_intents)
            yield intent_queue_send, receive_output_processed
