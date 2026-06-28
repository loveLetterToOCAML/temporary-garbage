from typing import Generic, TypeVar

from anyio.abc import ObjectReceiveStream, ObjectSendStream
from pydantic import BaseModel

from basetypes.implementation.basetypes_match import DefaultBaseType
from filer.base_exceptions import NotExistingContent, HashNotMatchingContent, AlreadyUploadedContent, \
    NotEnoughSpaceRemaining, OutOfSpaceConstraints, PermanentContent
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore

from contextlib import asynccontextmanager

from attrs import define

from filer.base_types import GetContentSizeIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent, GetContentIntent, UploadContentIntent, DeleteContentIntent


FilerRegistryIntent = GetContentSizeIntent | GetContentUlidForHashIntent | GetContentHashForUlidIntent | CheckContentForHashAndUlidIntent | \
    GetContentIntent | UploadContentIntent | DeleteContentIntent # | \
    # GetContentMetadata | FindContentForMetadata | CheckIntegrity | CheckBackend

FilerRegistryResult = QueryResult[int, NotExistingContent] | \
                      QueryResult[DefaultBaseType.ULID, NotExistingContent] | \
                      QueryResult[bytes, NotExistingContent] | \
                      QueryResult[bool, NotExistingContent | HashNotMatchingContent] | \
                      QueryResult[GetContentTicket, NotExistingContent] | \
                      QueryResult[UploadContentTicket, AlreadyUploadedContent | NotEnoughSpaceRemaining | OutOfSpaceConstraints] | \
                      QueryResult[bool, NotExistingContent | PermanentContent]


class FilerRegistryConfig(BaseModel):
    registryCacheSize = 0x100000



T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')
W = TypeVar('W')

@define
class CommonInputStream(Generic[T, U, V]):
    inputData: ObjectReceiveStream[T] | None = None
    inputAdmin: ObjectReceiveStream[U] | None = None
    inputInteraction: ObjectReceiveStream[V] | None = None

class CommonOutputStream(Generic[T, U, V, W]):
    outputData: ObjectSendStream[T] | None = None
    outputAdmin: ObjectSendStream[U] | None = None
    outputInteraction: ObjectSendStream[V] | None = None
    outputException: ObjectSendStream[W] | None = None

class ExecutionSystemParameters(BaseModel):
    inputWiring: CommonInputStream
    outputWiring: CommonOutputStream

class FilerRegistryParameters(ExecutionSystemParameters):
    backends: list[FilerBackend]


class FilerRegistry(AsyncContextManagerMixin):

    def __init__(self, params: FilerRegistryParameters):
        self._params = params
        self._task_group = None
        self._cache_backends = {}  # the ones which does not allow write
        self._writeable_backends = {}
        self._ordered_backends_for_read = {}

    async def _ask_first_backend_for(self, intent: FilerRegistryIntent):
        for backend in self._back:
            await backend.send()

    async def _process_intent(self, intent: FilerRegistryIntent):
        match intent:
            case GetContentSize():
                pass
            case GetContentUlidForHash():
                pass
            case GetContentHashForUlid():
                pass
            case CheckContentForHashAndUlid():
                pass
            case GetContent():
                pass
            case UploadContent():
                pass
            case DeleteContent():
                pass
            case _:
                raise NotImplementedError

    async def _process_intent_and_send_result(self, intent):
        response = await self._process_intent(intent)
        await self._internal_result_stream.send(response)

    async def _process_intents(self, task_group):
        if not self._receive_intent:
            raise NotInAsyncContextManager('_process_intents', 'FilerRegistry')

        async with (
            self._receive_intent,
            self._internal_result_stream
        ):
            async for intent in self._receive_intent:
                async with self._semaphore:
                    task_group.start_soon(self._process_intent_and_send_result, intent)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._internal_result_stream, external_receive_results = create_memory_object_stream[FilerRegistryResult](0x40)
        external_send_intent, self._receive_intent = create_memory_object_stream[FilerRegistryIntent](0x40)
        self._semaphore = Semaphore(0x10)

        async with (
            create_task_group() as task_group,
            self._backends
        ):
            task_group.start_soon(self._process_intents, task_group)
            yield external_send_intent
