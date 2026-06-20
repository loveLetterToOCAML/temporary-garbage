from __future__ import annotations

from baseimplems.anyio_utils import run_within
from baseimplems.datastreams.stream_event import StreamEvent, StreamStarting, base_event_from, \
    StreamEnding, StreamEndReason, BytesStreamEvent, TransitStatus, ChunkStreamEvent, ObjectStreamEvent, SleepEvent, \
    StreamIdentifier, StreamEventType
from basetests.objects_consumer import stream_event_consumer_max_events
from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root_params import RootSerial
from baseimplems.date_utils import utc_now

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, get_cancelled_exc_class
from pydantic import BaseModel
import anyio

from contextlib import asynccontextmanager, _AsyncGeneratorContextManager
from typing import Type, Dict, Callable, Awaitable, TypeVar
from typing_extensions import Generic
from contextvars import ContextVar
from datetime import timedelta
from random import randint


default_test_print_stream_event = print

print_with_threshold = stream_event_consumer_max_events(print, is_sync=True)


class TimeStat(BaseModel):
    perSecondMean: float
    perSecondDeviation: float

class GlobalStreamStats(BaseModel):
    currentlyRunning: int
    finished: int
    timeStats: TimeStat


T = TypeVar('T')

class StatPerStatus(BaseModel, Generic[T]):
    successfullyExchanged: T = 0
    queued: T = 0
    inProgress: T = 0
    failed: T = 0
    retrying: T = 0

class StatsPerStatus(BaseModel):
    rawPackets: StatPerStatus[int]
    perSecondMeans: StatPerStatus[float]
    perSecondDeviations: StatPerStatus[float]


StatsPerType = Dict[StreamEventType, StatsPerStatus | TimeStat]
StatsPerStream = Dict[StreamIdentifier, StatsPerType]


class StatsForStream:

    def __init__(self):
        self._stream_infos = {}
        self._stream_stats = {}
        self._bytes_stats = {}
        self._chunk_stats = {}
        self._object_stats = {}
        self._sleep_stats = {}

    async def __call__(self, streaming_event: StreamEvent):
        stream_key = (streaming_event.name, streaming_event.index)
        self._stream_infos.setdefault(stream_key, streaming_event)
        print(streaming_event)
        return
        match streaming_event:
            case BytesStreamEvent():
                self._update_dict(self._bytes_stats)
            case ChunkStreamEvent():
                self._update_dict(self._chunk_stats)
            case ObjectStreamEvent():
                self._update_dict(self._object_stats)
            case StreamStarting():
                self._update_stream_starting(self._stream_stats)
            case StreamEnding():
                self._update_stream_ending(self._stream_stats)
            case SleepEvent():
                self._update_dict(self._sleep_stats)
            case _:
                print("other", type(streaming_event))

current_stats_stream = ContextVar[StatsForStream]('stats_stream')
run_with_stats_stream = run_within(StatsForStream, current_stats_stream)


class StreamEventStream:

    def __init__(self, name, index, date_creation, additional_discriminant):
        self._stream_infos = {
            'name': f"{name}-{additional_discriminant}",
            'index': index
        }
        self._absolute_creation_time = date_creation

    async def _send_to_collector(self, event: StreamEvent):
        await stream_event_collector.get().emit(event)

    def _internal_new_event(self, EventType: Type, **kwargs):
        return EventType(**kwargs, **self._stream_infos, **base_event_from(self._absolute_creation_time))

    async def _internal_new_event_send(self, EventType: Type, **kwargs):
        await self._send_to_collector(self._internal_new_event(EventType, **kwargs))

    async def new_stream_start_event(self, details: RootSerial | None = None):
        await self._internal_new_event_send(StreamStarting, details=details)

    async def new_stream_end_event(self, stream_end_reason: StreamEndReason):
        await self._internal_new_event_send(StreamEnding, reason=stream_end_reason)


    def _event_with_status(self, status: TransitStatus, attempt: int = 1, details: RootSerial | None = None):
        return {
            'status': status,
            'attempt': attempt,
            'details': details
        }

    async def new_bytes_event(self, offset: int, size: int, status: TransitStatus, attempt: int = 1, details: RootSerial | None = None):
        await self._internal_new_event_send(BytesStreamEvent, offset=offset, size=size, **self._event_with_status(status, attempt, details))

    async def new_chunk_event(self, index: int, offset: int, size: int, status: TransitStatus, attempt: int = 1, details: RootSerial | None = None):
        await self._internal_new_event_send(ChunkStreamEvent, index=index, offset=offset, size=size, **self._event_with_status(status, attempt, details))

    async def new_object_event(self, index: int, type: DefaultBaseType.TYPE, status: TransitStatus, attempt: int = 1, details: RootSerial | None = None):
        await self._internal_new_event_send(ObjectStreamEvent, index=index, type=type, **self._event_with_status(status, attempt, details))

    async def new_pause_event(self, delay: timedelta):
        await self._internal_new_event_send(SleepEvent, delay=delay)


class NotInAsyncContextManager(Exception):

    def __init__(self, method_name, class_name):
        super().__init__(f"`{method_name}` function of {class_name} intends to be executed within `async with [{class_name}_instance]`")


class EventCollector(AsyncContextManagerMixin):

    def __init__(self, sync_handlers: list[Callable[[StreamEvent], None]],
                 async_handlers: list[Callable[[StreamEvent], Awaitable[None]]],
                 max_number_of_tasks: int = 0x20, max_buffer_capacity=0x1000):
        self._current_stream_index = 0
        self._sync_handlers = sync_handlers
        self._async_handlers = async_handlers
        self._handlers_ok = {}
        self._limiter = anyio.CapacityLimiter(max_number_of_tasks)
        self._send_event, self._on_receive = create_memory_object_stream[StreamEvent](max_buffer_size=0x1000)

    async def _consumer(self):
        async with self._on_receive:
            async for data in self._on_receive:
                for sync_handler in self._sync_handlers:
                    if self._handlers_ok[(sync_handler, True)]:
                        try:
                            sync_handler(data)
                        except Exception as e:
                            self._handlers_ok[(sync_handler, True)] = False
                for async_handler in self._async_handlers:
                    async with self._limiter:
                        if self._handlers_ok[(async_handler, False)]:
                            try:
                                await async_handler(data)
                            except Exception as e:
                                self._handlers_ok[(async_handler, False)] = False

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        async with (
            create_task_group() as self._task_group,
            self._send_event
        ):
            for hdlr in self._sync_handlers:
                self._handlers_ok[(hdlr, True)] = True
            for hdlr in self._async_handlers:
                self._handlers_ok[(hdlr, False)] = True
            self._task_group.start_soon(self._consumer)
            yield self

    async def emit(self, event: StreamEvent):
        if (self._sync_handlers or self._async_handlers) and not self._handlers_ok:
            raise NotInAsyncContextManager('emit', 'EventCollector')
        await self._send_event.send(event)

    def next_stream_event_collector(self, f_name: str) -> StreamEventStream:
        idx, self._current_stream_index = self._current_stream_index, self._current_stream_index + 1
        return StreamEventStream(f_name, idx, utc_now(), hex(randint(0, 0xffffffff)))


@asynccontextmanager
async def default_prepare_event_handlers_context():
    async with (
        run_with_stats_stream(),
        print_with_threshold as send_to_print_with_threshold
    ):
        yield {'async_handlers': [current_stats_stream.get, send_to_print_with_threshold]}

stream_event_collector = ContextVar[EventCollector]('stream_events')
run_with_event_collector = run_within(
    EventCollector,
    stream_event_collector,
    default_bind_static_arguments = {
        'sync_handlers': [default_test_print_stream_event],
    },
    upper_context_dependency=default_prepare_event_handlers_context
)
current_stream_event_stream = ContextVar[StreamEventStream]('current_stream_event_stream')



@asynccontextmanager
async def next_stream_event_collector(f_name, send_start_event: bool = True):
    csec = stream_event_collector.get()
    sec = csec.next_stream_event_collector(f_name)
    prev = current_stream_event_stream.set(sec)
    reason = StreamEndReason.END_OF_INPUT
    try:
        with anyio.CancelScope() as inner:  # just to know case of stream stop (distinguisher between external and internal)
            if send_start_event:
                await sec.new_stream_start_event()
        yield sec
    except get_cancelled_exc_class():
        print("CAUGHT", inner.cancelled_caught)
        reason = StreamEndReason.EXCEPTION_DURING_PROCESS
        raise
    finally:
        await sec.new_stream_end_event(reason)
        current_stream_event_stream.reset(prev)
