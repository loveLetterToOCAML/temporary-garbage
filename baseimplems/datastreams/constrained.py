import time
from contextlib import asynccontextmanager
from typing import AsyncIterable

import anyio
from anyio import AsyncContextManagerMixin, create_memory_object_stream, create_task_group, move_on_after, CancelScope
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from pydantic import BaseModel

from baseimplems.datastreams.event_processing import run_with_stats_stream
from baseimplems.datastreams.stream_event import StreamEvent


class StreamConstraints(BaseModel):
    maximumStreamDurationSeconds: float
    bootstrapDelaySeconds: float
    minBytesPerSecond: float
    maxBytesPerSecond: float
    backoffDelaySeconds: float
    toleratedFaults: int
    resetFaultsDelaySeconds: float


class ConstraintInformation(BaseModel):
    remainingDurationSeconds: float
    currentBytesPerSecond: float
    currentFaulted: int
    remainingFaults: int
    delayBeforeFaultResetSeconds: float


class StreamWithConstraints(AsyncContextManagerMixin):

    def __init__(self, params: StreamConstraints):
        self._params = params
        self._total_time_max = self._params.bootstrapDelaySeconds + self._params.maximumStreamDurationSeconds

    def _construct_constraint_information(self):
        remaining = self._total_time_max - (time.monotonic() - self._started_at)
        remaining = 0 if remaining < 0 else remaining
        return ConstraintInformation(
            remainingDurationSeconds=remaining,
            currentBytesPerSecond=self._cur_throughput,
            currentFaulted=self._cur_faults,
            remainingFaults=self._params.toleratedFaults - self._cur_faults,
            delayBeforeFaultResetSeconds=self._params.resetFaultsDelaySeconds - (time.monotonic() - self._last_fault)
        )

    async def _update_stream_stats(self, stream_event: StreamEvent, local_send_constraint_info: MemoryObjectSendStream):
        await local_send_constraint_info.send()

    async def _process_stream_event_loop(self, cancel_scope: CancelScope, local_receive_stream, local_send_constraint_info: MemoryObjectSendStream):
        async with (
            local_receive_stream,
        ):
            async for stream_event in local_receive_stream:
                await self._update_stream_stats(stream_event, local_send_constraint_info)
            cancel_scope.cancel('remote peer disconnected')

    async def _handle_global_timeout(self, cancel_scope: CancelScope, local_send_constraint_info: MemoryObjectSendStream):
        await anyio.sleep(self._params.bootstrapDelaySeconds + self._params.maximumStreamDurationSeconds)
        await local_send_constraint_info.send(self._construct_constraint_information())
        # await remote_send_stream.aclose()
        # await local_send_constraint_info.aclose()
        cancel_scope.cancel('global timeout')

    async def _handle_stream_constraints(self, cancel_scope: CancelScope, local_send_constraint_info: MemoryObjectSendStream):
        await anyio.sleep(self._params.bootstrapDelaySeconds)
        while True:
            await anyio.sleep(self._next_delay)

        await local_send_constraint_info.send(self._construct_constraint_information())
        cancel_scope.cancel('constraints not fulfilled')

    async def _transfer_constraint_info(self, local_receive_orders: AsyncIterable, local_send_constraint_info: MemoryObjectSendStream):
        async for _ in local_receive_orders:
            await local_send_constraint_info.send(self._construct_constraint_information())

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        remote_send_stream, local_receive_stream = create_memory_object_stream[StreamEvent](max_buffer_size=0x100)

        remote_send_constraint_intent, local_receive_orders = create_memory_object_stream[bool](max_buffer_size=0x10)
        local_send_constraint_info, remote_receive_constraint_info = create_memory_object_stream[ConstraintInformation](max_buffer_size=0x100)

        async with (
            create_task_group() as tg,
            local_receive_stream,
            local_receive_orders,
            local_send_constraint_info
        ):
            self._next_delay = 0
            self._started_at = time.monotonic()
            self._cur_throughput = 0
            self._cur_faults = 0
            self._last_fault = -1
            with CancelScope() as cancel_scope:
                tg.start_soon(self._transfer_constraint_info, local_receive_orders, local_send_constraint_info)
                tg.start_soon(self._process_stream_event_loop, cancel_scope, local_receive_stream, local_send_constraint_info)
                tg.start_soon(self._handle_global_timeout, cancel_scope, local_send_constraint_info)
                tg.start_soon(self._handle_stream_constraints, cancel_scope, local_send_constraint_info)
                yield remote_send_stream, remote_send_constraint_intent, remote_receive_constraint_info


@asynccontextmanager
async def default_prepare_event_handlers_context():
    async with (
        run_with_stats_stream(),
    ):
        yield {'async_handlers': [current_stats_stream.get(), send_to_print_with_threshold.send]}

stream_event_collector = ContextVar[EventCollector]('stream_events')
run_with_event_collector = run_within(
    EventCollector,
    stream_event_collector,
    #default_bind_static_arguments = {
    #    'sync_handlers': [default_test_print_stream_event],
    #},
    upper_context_dependency=default_prepare_event_handlers_context
)
current_stream_event_stream = ContextVar[StreamEventStream]('current_stream_event_stream')
