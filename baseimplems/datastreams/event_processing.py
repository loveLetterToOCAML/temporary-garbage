from contextlib import asynccontextmanager
from datetime import datetime
from enum import IntFlag

from anyio import create_task_group, create_memory_object_stream, AsyncContextManagerMixin

from baseimplems.datastreams.stream_event import StreamEventType, StreamIdentifier, StreamEvent, TransitStatus, \
    BytesStreamEvent, ChunkStreamEvent, ObjectStreamEvent, StreamStarting, StreamEnding, SleepEvent
from basetests.objects_consumer import stream_event_consumer_max_events
from baseimplems.anyio_utils import run_within

from pydantic import BaseModel

from typing import TypeVar, Generic, Dict, List
from contextvars import ContextVar
import math


default_test_print_stream_event = print

print_with_threshold = stream_event_consumer_max_events(print, is_sync=True)


class TimeStat(BaseModel):
    n: int = 0
    perSecondMean: float = 0.0
    perSecondDeviation: float = 0.0

    def update(self, v: float) -> None:
        self.n += 1
        delta = v - self.perSecondMean
        self.perSecondMean += delta / self.n
        self.perSecondDeviation += delta * (v - self.perSecondMean)

    @property
    def variance(self) -> float:
        return self.perSecondDeviation / (self.n - 1) if self.n >= 2 else 0.0

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)


class GlobalStreamStats(BaseModel):
    currentlyRunning: int = 0
    finished: int = 0
    timeStats: TimeStat = TimeStat()


T = TypeVar('T')

class StatPerStatus(BaseModel, Generic[T]):
    successfullyExchanged: T = 0
    queued: T = 0
    inProgress: T = 0
    failed: T = 0
    retrying: T = 0

def default_time_stats():
    return StatPerStatus[TimeStat](
        successfullyExchanged=TimeStat(),
        queued=TimeStat(),
        inProgress=TimeStat(),
        failed=TimeStat(),
        retrying=TimeStat(),
    )

class StatsPerStatus(BaseModel):
    rawPackets: StatPerStatus[int] = StatPerStatus[int]()
    timeStats: StatPerStatus[TimeStat] = default_time_stats()


StatsPerType = Dict[StreamEventType, StatsPerStatus | TimeStat]
StatsPerStream = Dict[StreamIdentifier, StatsPerType]


class StatsForStream(AsyncContextManagerMixin):

    def __init__(self):
        self._stream_infos = {}
        self._stream_statuses = {}
        self._stream_stats: StatsPerStream = {}
        self._global_stats_per_type: StatsPerType = {
            StreamEventType.BYTES_EVENT: StatsPerStatus(),
            StreamEventType.CHUNK_EVENT: StatsPerStatus(),
            StreamEventType.OBJECT_EVENT: StatsPerStatus(),
            StreamEventType.SLEEP_EVENT: TimeStat(),
        }
        self._global_stats: GlobalStreamStats = GlobalStreamStats()

    def _update_dict(self, stats_dict, event_status: TransitStatus, increase_of: int):
        if event_status == TransitStatus.DONE:
            stats_dict.rawPackets.successfullyExchanged += increase_of
            time_stats = stats_dict.timeStats.successfullyExchanged
        elif event_status == TransitStatus.QUEUED:
            stats_dict.rawPackets.queued += increase_of
            time_stats = stats_dict.timeStats.queued
        elif event_status == TransitStatus.SENDING:
            stats_dict.rawPackets.inProgress += increase_of
            time_stats = stats_dict.timeStats.inProgress
        elif event_status == TransitStatus.FAILED:
            stats_dict.rawPackets.failed += increase_of
            time_stats = stats_dict.timeStats.failed
        elif event_status == TransitStatus.RETRYING:
            stats_dict.rawPackets.retrying += increase_of
            time_stats = stats_dict.timeStats.retrying
        else:
            raise NotImplementedError
        time_stats.update(increase_of)

    def _update_all_dicts(self, stream_index, event_type: StreamEventType, event_status: TransitStatus, increase_of: int):
        self._stream_stats.setdefault(
            stream_index, {
                StreamEventType.BYTES_EVENT: StatsPerStatus(),
                StreamEventType.CHUNK_EVENT: StatsPerStatus(),
                StreamEventType.OBJECT_EVENT: StatsPerStatus(),
                StreamEventType.SLEEP_EVENT: TimeStat(),
            }
        )
        self._update_dict(self._stream_stats[stream_index][event_type], event_status, increase_of)
        self._update_dict(self._global_stats_per_type[event_type], event_status, increase_of)
        print(self._stream_stats)

    async def __call__(self, streaming_event: StreamEvent):
        stream_key = hash(streaming_event)
        self._stream_infos.setdefault(stream_key, streaming_event)
        if stream_key not in self._stream_infos and not isinstance(streaming_event, StreamStarting):
            print(f"Warning should not happen, {stream_key} not in started streams")
            return
        match streaming_event:
            case BytesStreamEvent():
                self._update_all_dicts(stream_key, streaming_event.type, streaming_event.status, streaming_event.size)
            case ChunkStreamEvent():
                self._update_all_dicts(stream_key, streaming_event.type, streaming_event.status, streaming_event.size)
            case ObjectStreamEvent():
                self._update_all_dicts(stream_key, streaming_event.type, streaming_event.status, 1)
            case StreamStarting():
                self._stream_infos.setdefault(stream_key, streaming_event)
                if stream_key in self._stream_statuses:
                    print(f"Warning should not happen, stream {stream_key} duplicate")
                    return
                self._stream_statuses[stream_key] = StreamStatus(
                    name=streaming_event.name,
                    index=streaming_event.index,
                    randomId=streaming_event.randomId,
                    isRunning=True,
                    startedAt=streaming_event.absTime
                )
                self._global_stats.currentlyRunning += 1
                self._global_stats.timeStats.update(1)
            case StreamEnding():
                if not self._stream_statuses[stream_key].isRunning:
                    print(f"Warning should not happen, stream {stream_key} already ended")
                    return
                self._stream_statuses[stream_key].isRunning = False
                self._stream_statuses[stream_key].stoppedAt = streaming_event.absTime
                self._global_stats.finished += 1
            case SleepEvent():
                self._update_all_dicts(stream_key, streaming_event.type, TransitStatus.DONE, 1)
            case _:
                raise NotImplementedError


    def process_intent(self, intent):
        stream_infos = stream_stats = stats_per_type = global_stats = None

        if intent.intentType & StatsIntentType.STREAM_INFOS.value == StatsIntentType.STREAM_INFOS.value:
            stream_infos = list(self._stream_statuses.values())

        if intent.intentType & StatsIntentType.STREAM_STATS.value == StatsIntentType.STREAM_STATS.value:
            stream_stats = self._stream_stats

        if intent.intentType & StatsIntentType.GLOBAL_STATS_PER_TYPE.value == StatsIntentType.GLOBAL_STATS_PER_TYPE.value:
            stats_per_type = self._global_stats_per_type

        if intent.intentType & StatsIntentType.GLOBAL_STATS.value == StatsIntentType.GLOBAL_STATS.value:
            global_stats = self._global_stats

        return StatsOutput(
            streamInfos=stream_infos,
            streamStats=stream_stats,
            statsPerType=stats_per_type,
            globalStats=global_stats
        )


    @asynccontextmanager
    async def __asynccontextmanager__(self):
        remote_send_orders, local_receive_orders = create_memory_object_stream[StatsIntent](max_buffer_size=0)
        local_send_stats, remote_receive_stats = create_memory_object_stream[StatsPerStream](max_buffer_size=0)

        async def process():
            async with (
                create_task_group() as self._task_group,
                local_receive_orders,
                local_send_stats
            ):
                async for intent in local_receive_orders:
                    await local_send_stats.send(self.process_intent(intent))

        async with create_task_group() as tg:
            tg.start_soon(process)
            yield remote_send_orders, remote_receive_stats


class StatsIntentType(IntFlag):
    STREAM_INFOS = 1
    STREAM_STATS = 2
    GLOBAL_STATS = 4
    GLOBAL_STATS_PER_TYPE = 8

class StatsIntent(BaseModel):
    intentType: StatsIntentType
    filterEventType: StreamEventType | None = None
    filterStream: StreamIdentifier | None = None
    filterTransitStatus: TransitStatus | None = None

class StreamStatus(StreamIdentifier):
    isRunning: bool
    startedAt: datetime
    stoppedAt: datetime | None = None

class StatsOutput(BaseModel):
    streamInfos: List[StreamStatus] | None = None
    streamStats: StatsPerStream | None = None
    statsPerType: StatsPerType | None = None
    globalStats: GlobalStreamStats | None = None


current_stats_stream = ContextVar[StatsForStream]('stats_stream')
run_with_stats_stream = run_within(StatsForStream, current_stats_stream)
