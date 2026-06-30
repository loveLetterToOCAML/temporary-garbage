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



class FilerBackendInMem(AsyncContextManagerMixin):

    def __init__(self, params: FilerBackendInMemParameters):
        self._params = params

    async def _process_intent(self, intent: FilerBackendIntent):
        match intent:
            case GetContentSizeIntent():
                pass
            case GetContentUlidForHashIntent():
                pass
            case GetContentHashForUlidIntent():
                pass
            case CheckContentForHashAndUlidIntent():
                pass
            case GetContentIntent():
                pass
            case UploadContentIntent():
                pass
            case UploadChunkIntent():
                pass
            case UploadProgressIntent():
                pass
            case DeleteContentIntent():
                pass
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
        self._semaphore = Semaphore(0x10)

        async with (
            create_task_group() as task_group,
        ):
            task_group.start_soon(self._process_intents, task_group)
            yield external_send_intent
