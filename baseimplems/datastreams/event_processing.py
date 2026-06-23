from baseimplems.datastreams.stream_event import StreamEventType, StreamIdentifier, StreamEvent, TransitStatus, \
    BytesStreamEvent, ChunkStreamEvent, ObjectStreamEvent, StreamStarting, StreamEnding, SleepEvent
from basetests.objects_consumer import stream_event_consumer_max_events
from baseimplems.anyio_utils import run_within

from anyio import create_task_group, create_memory_object_stream, AsyncContextManagerMixin
from pydantic import BaseModel

from typing import TypeVar, Generic, Dict, List
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from enum import IntFlag
import math
import time


default_test_print_stream_event = print

print_with_threshold = stream_event_consumer_max_events(print, is_sync=True)


@dataclass
class TimeStatInternal:
    # per-event
    n:            int   = 0
    _mean_evt:    float = 0.0
    _M2_evt:      float = 0.0
    # per-second
    t_last:       float = 0.0
    total_time:   float = 0.0
    total_bytes:  float = 0.0
    _M2_rate:     float = 0.0
    _w_mean:      float = 0.0


    def update(self, nbytes: float) -> None:
        t = time.monotonic()

        # 1. per-event Welford (always)
        if nbytes > 0:
            self.n += 1
            delta = nbytes - self._mean_evt
            self._mean_evt += delta / self.n
            self._M2_evt += (delta * (nbytes - self._mean_evt) - self._M2_evt) / self.n

        # 2. per-second: seed on first event
        if self.n == 1:
            self.t_last = t
            self.total_bytes = nbytes
            return

        dt = t - self.t_last
        if dt < 0:
            raise ValueError(f"non-monotonic time: dt={dt}")

        self.total_bytes += nbytes

        if dt == 0:
            return  # simultaneous: bytes counted, no new rate sample

        self.total_time += dt
        self.t_last = t
        old_mean = self._w_mean
        rate = nbytes/dt
        self._w_mean = old_mean + (dt / self.total_time) * (rate - self._w_mean)
        self._M2_rate += dt * (rate - old_mean) * (rate - self._w_mean)

    @property
    def mean_bytes_per_event(self) -> float:
        return self._mean_evt

    @property
    def std_bytes_per_event(self) -> float:
        return math.sqrt(self._M2_evt / self.n) if self.n >= 1 else 0.0

    @property
    def mean_bytes_per_sec(self) -> float:
        return self.total_bytes / self.total_time if self.total_time > 0 else 0.0

    @property
    def std_bytes_per_sec(self) -> float:
        return math.sqrt(self._M2_rate / self.total_time) if self.total_time > 0 else 0.0

    @property
    def public(self):
        return TimeStat(
            perEventMean=self.mean_bytes_per_event,
            perEventDeviation=self.std_bytes_per_event,
            perSecondMean = self.mean_bytes_per_sec,
            perSecondDeviation=self.std_bytes_per_sec,
        )


class TimeStat(BaseModel):
    perEventMean: float
    perEventDeviation: float
    perSecondMean: float
    perSecondDeviation: float


class GlobalStreamStats(BaseModel):
    currentlyRunning: int = 0
    finished: int = 0
    timeStats: TimeStatInternal | TimeStat = TimeStatInternal()


T = TypeVar('T')

class StatPerStatus(BaseModel, Generic[T]):
    successfullyExchanged: T = 0
    queued: T = 0
    inProgress: T = 0
    failed: T = 0
    retrying: T = 0

def default_time_stats():
    return StatPerStatus[TimeStatInternal](
        successfullyExchanged=TimeStatInternal(),
        queued=TimeStatInternal(),
        inProgress=TimeStatInternal(),
        failed=TimeStatInternal(),
        retrying=TimeStatInternal(),
    )

class StatsPerStatus(BaseModel):
    rawPackets: StatPerStatus[int] = StatPerStatus[int]()
    timeStats: StatPerStatus[TimeStat] = default_time_stats()

def convert_to_public(data: TimeStatInternal | StatsPerStatus) -> TimeStat | StatsPerStatus:
    match data:
        case TimeStatInternal():
            return data.public
        case StatsPerStatus():
            return StatsPerStatus(
                rawPackets=data.rawPackets,
                timeStats=StatPerStatus[TimeStat](
                    successfullyExchanged=data.timeStats.successfullyExchanged.public,
                    queued=data.timeStats.queued.public,
                    inProgress=data.timeStats.inProgress.public,
                    failed=data.timeStats.failed.public,
                    retrying=data.timeStats.retrying.public,
                )
            )
        case _:
            raise NotImplementedError


StatsPerType = Dict[StreamEventType, StatsPerStatus | TimeStat]
StatsPerStream = Dict[StreamIdentifier, StatsPerType]


class StatsForStream:

    def __init__(self):
        self._stream_infos = {}
        self._stream_statuses = {}
        self._stream_stats: StatsPerStream = {}
        self._global_stats_per_type: StatsPerType = {
            StreamEventType.BYTES_EVENT: StatsPerStatus(),
            StreamEventType.CHUNK_EVENT: StatsPerStatus(),
            StreamEventType.OBJECT_EVENT: StatsPerStatus(),
            StreamEventType.SLEEP_EVENT: TimeStatInternal(),
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
                StreamEventType.SLEEP_EVENT: TimeStatInternal(),
            }
        )
        self._update_dict(self._stream_stats[stream_index][event_type], event_status, increase_of)
        self._update_dict(self._global_stats_per_type[event_type], event_status, increase_of)

    async def __call__(self, streaming_event: StreamEvent):
        stream_key = hash(streaming_event)
        self._stream_infos.setdefault(stream_key, streaming_event)
        if stream_key not in self._stream_infos and not isinstance(streaming_event, StreamStarting):
            print(f"Warning should not happen, {stream_key} not in started streams")
            return
        match streaming_event:
            case BytesStreamEvent():
                self._update_all_dicts(stream_key, streaming_event.type, streaming_event.status, streaming_event.size)
                self._global_stats.timeStats.update(0)
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
                self._stream_statuses[stream_key] = StreamStatus(
                    name=self._stream_statuses[stream_key].name,
                    index=self._stream_statuses[stream_key].index,
                    randomId=self._stream_statuses[stream_key].randomId,
                    isRunning=False,
                    startedAt=self._stream_statuses[stream_key].startedAt,
                    stoppedAt = streaming_event.absTime
                )
                self._global_stats.finished += 1
            case SleepEvent():
                self._update_all_dicts(stream_key, streaming_event.type, TransitStatus.DONE, 1)
            case _:
                raise NotImplementedError


class StatsForStreamProcessing(AsyncContextManagerMixin):

    def process_intent(self, intent):
        stream_infos = stream_stats = stats_per_type = global_stats = None

        if intent.intentType & StatsIntentType.STREAM_INFOS.value == StatsIntentType.STREAM_INFOS.value:
            stream_infos = list(self._stats_stream._stream_statuses.values())

        if intent.intentType & StatsIntentType.STREAM_STATS.value == StatsIntentType.STREAM_STATS.value:
            stream_stats = {
                self._stats_stream._stream_infos[k]: {
                    k2: convert_to_public(v2) for k2, v2 in v.items()
                } for k, v in self._stats_stream._stream_stats.items()
            }

        if intent.intentType & StatsIntentType.GLOBAL_STATS_PER_TYPE.value == StatsIntentType.GLOBAL_STATS_PER_TYPE.value:
            stats_per_type = {
                k: convert_to_public(v) for k, v in self._stats_stream._global_stats_per_type.items()
            }

        if intent.intentType & StatsIntentType.GLOBAL_STATS.value == StatsIntentType.GLOBAL_STATS.value:
            global_stats = GlobalStreamStats(
                **{
                    **self._stats_stream._global_stats.dict(),
                    'timeStats': self._stats_stream._global_stats.timeStats.public
                }
            )

        return StatsOutput(
            streamInfos=stream_infos,
            streamStats=stream_stats,
            statsPerType=stats_per_type,
            globalStats=global_stats
        )

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._stats_stream = current_stats_stream.get()

        remote_send_orders, local_receive_orders = create_memory_object_stream[StatsIntent](max_buffer_size=0)
        local_send_stats, remote_receive_stats = create_memory_object_stream[StatsPerStream](max_buffer_size=0)

        async def process():
            async with (
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
