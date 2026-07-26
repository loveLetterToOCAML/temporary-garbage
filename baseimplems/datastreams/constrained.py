from baseimplems.datastreams.stream_event import StreamEvent, event_payload_size, StreamStarting, StreamInProgress, \
    StreamEnding, base_event_from, StreamEndReason, BytesStreamEvent, TransitStatus
from baseimplems.datastreams.observers import StatePoller
from baseimplems.date_utils import utc_now

from anyio import AsyncContextManagerMixin, create_memory_object_stream, create_task_group, CancelScope, Event
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import BaseModel
import anyio

from contextlib import asynccontextmanager
from contextvars import ContextVar
from random import randint
import time


class StreamConstraints(BaseModel):
    maximumStreamDurationSeconds: float
    bootstrapDelaySeconds: float
    minBytesPerSecond: float
    maxBytesPerSecond: float
    probeDelaySeconds: float    # normal delay between two watch probes
    backoffDelaySeconds: float  # delay in case of fault
    toleratedFaults: int
    resetFaultsDelaySeconds: float  # delay before fault counter is reset


class StreamInformation(BaseModel):
    remainingDurationSeconds: float
    currentBytesPerSecond: float
    totalBytes: int
    currentFaulted: int
    remainingFaults: int
    delayBeforeFaultResetSeconds: float
    status: StreamStarting | StreamInProgress | StreamEnding


current_watchguard_index = ContextVar[int]('current_stream_watchguard_index', default=0)


class ConstraintsHandler:

    def __init__(self, params: StreamConstraints, name: str, stream_index: int):
        self._params = params
        self._total_time_max = self._params.bootstrapDelaySeconds + self._params.maximumStreamDurationSeconds
        self._stream_identifier = {
            'name': name,
            'index': stream_index,
            'randomId': randint(0, 0xffffffff),
        }
        self._creation_time = utc_now()
        self._current_stream_status = StreamStarting(**self._stream_identifier, **base_event_from())
        self._start_constraint_max = False
        self._next_delay = 0
        self._started_at = time.monotonic()
        self._cur_throughput = 0
        self._cur_faults = 0
        self._last_fault = -1
        self._total_bytes = 0
        self._bootstrapped = -1

    @property
    def stream_identifier(self):
        return dict(self._stream_identifier)  # copy it to avoid accidental internal modification

    def _update_end_status(self, cancel_scope: CancelScope, end_reason: StreamEndReason, end_reason_str: str):
        if not self._current_stream_status:  # first one only is to decide which is the real stop reason
            self._current_stream_status = StreamEnding(
                **self._stream_identifier, **base_event_from(self._creation_time),
                reason=end_reason, details=end_reason_str
            )
        cancel_scope.cancel(end_reason_str)

    def _update_stream_stats(self, stream_event: StreamEvent | None = None):
        if self._start_constraint_max:
            elapsed = time.monotonic() - self._bootstrapped
        else:
            elapsed = time.monotonic() - self._started_at
        if stream_event:
            self._total_bytes += event_payload_size(stream_event)
        if elapsed > 0:
            self._cur_throughput = self._total_bytes / elapsed
        else:
            self._cur_throughput = 0

    async def _handle_stream_constraints(self, cancel_scope: CancelScope, msg: str | None = None):
        await anyio.sleep(self._params.bootstrapDelaySeconds)
        self._bootstrapped = time.monotonic()
        while True:
            if self._cur_faults > self._params.toleratedFaults:
                break

            await anyio.sleep(self._next_delay)
            self._update_stream_stats()

            if not self._start_constraint_max and time.monotonic() - self._started_at < 1.5 * (time.monotonic() - self._bootstrapped):
                self._start_constraint_max = True

            if self._cur_throughput < self._params.minBytesPerSecond or \
                    (self._cur_throughput > self._params.maxBytesPerSecond and self._start_constraint_max):
                self._cur_faults += 1
                self._last_fault = time.monotonic()
                self._next_delay = self._params.backoffDelaySeconds
                continue

            if self._cur_faults > 0 and time.monotonic() - self._last_fault > self._params.resetFaultsDelaySeconds:
                self._cur_faults = 0
            self._next_delay = self._params.probeDelaySeconds
        self._update_end_status(cancel_scope, StreamEndReason.EXCEPTION_DURING_PROCESS, msg or 'constraints not fulfilled')

    async def _handle_global_timeout(self, cancel_scope: CancelScope, msg: str | None = None):
        await anyio.sleep(self._params.bootstrapDelaySeconds + self._params.maximumStreamDurationSeconds)
        self._update_end_status(cancel_scope, StreamEndReason.EXCEPTION_DURING_PROCESS, msg or 'stream global timeout')

    def current_internal_state(self) -> StreamInformation:
        remaining = self._total_time_max - (time.monotonic() - self._started_at)
        remaining = 0 if remaining < 0 else remaining
        return StreamInformation(
            remainingDurationSeconds=remaining,
            currentBytesPerSecond=self._cur_throughput,
            totalBytes=self._total_bytes,
            currentFaulted=self._cur_faults,
            remainingFaults=self._params.toleratedFaults - self._cur_faults,
            delayBeforeFaultResetSeconds=(self._params.resetFaultsDelaySeconds - (time.monotonic() - self._last_fault)) if self._cur_faults else -1,
            status=self._current_stream_status if self._current_stream_status else \
                StreamInProgress(**self._stream_identifier, **base_event_from(self._creation_time))
        )


class StreamWatchguard(ConstraintsHandler, AsyncContextManagerMixin):

    def __init__(self, params: StreamConstraints, watchguard_name: str | None = None):
        idx = current_watchguard_index.get()
        current_watchguard_index.set(idx + 1)
        super().__init__(params, watchguard_name or 'stream-watchguard', idx)

    async def _process_stream_event_loop(self, cancel_scope: CancelScope, local_receive_stream: MemoryObjectReceiveStream[StreamEvent]):
        async with (
            local_receive_stream,
        ):
            async for stream_event in local_receive_stream:
                self._update_stream_stats(stream_event)
            self._update_end_status(cancel_scope, StreamEndReason.END_OF_INPUT, 'remote peer disconnected')

    async def _handle_external_end_event(self, cancel_scope: CancelScope, external_stream_end: Event):
        await external_stream_end.wait()
        self._update_end_status(cancel_scope, StreamEndReason.EXCEPTION_DURING_PROCESS, 'stream watchguard externally interrupted')

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        remote_send_stream, local_receive_stream = create_memory_object_stream[StreamEvent](max_buffer_size=0x100)
        external_stream_poller = StatePoller(self.current_internal_state)
        external_stream_end = Event()

        async with (
            external_stream_poller,
            local_receive_stream,
            create_task_group() as tg,
        ):
            self._current_stream_status = None
            self._started_at = time.monotonic()
            with CancelScope() as cancel_scope:
                tg.start_soon(self._process_stream_event_loop, cancel_scope, local_receive_stream)
                tg.start_soon(self._handle_stream_constraints, cancel_scope)
                tg.start_soon(self._handle_global_timeout, cancel_scope)
                tg.start_soon(self._handle_external_end_event, cancel_scope, external_stream_end)
                yield remote_send_stream, external_stream_poller, external_stream_end

            tg.cancel_scope.cancel()  # cancel task group after any condition


async def generate_stream_event(plan: dict[float, int], sender: MemoryObjectSendStream[bytes]):
    stream_identifier = {
        'name': 'stream-mock',
        'index': 0,
        'randomId': randint(0, 0xffffffff),
        **base_event_from()
    }
    cur_offset = 0
    for time_offset, sz in plan.items():
        print('[.] sleeping', time_offset - cur_offset, 'sending', sz, 'bytes')
        await anyio.sleep(time_offset - cur_offset)
        cur_offset = time_offset
        await sender.send(BytesStreamEvent(offset=0, size=sz, status=TransitStatus.DONE, **stream_identifier))


@asynccontextmanager
async def generate_stream_event_task(plan: dict[float, int], sender: MemoryObjectSendStream[bytes]):
    async with create_task_group() as tg:
        tg.start_soon(generate_stream_event, plan, sender)
        yield tg


if __name__ == '__main__':

    async def print_all(prefix, stream_poller):
        async for value in stream_poller:
            print(prefix, value)
            await anyio.sleep(0.5)

    async def main(params, plan: dict[float, int]):
        guard = StreamWatchguard(params)
        async with (
            guard as (remote_send_stream, external_stream_poller, external_stream_end),
            remote_send_stream,
            generate_stream_event_task(plan, remote_send_stream),
        ):
            await print_all("value while it works", external_stream_poller)
        print("last value", guard.current_internal_state())

    params1 = StreamConstraints(
        maximumStreamDurationSeconds=10,
        bootstrapDelaySeconds=1,
        minBytesPerSecond=2,
        maxBytesPerSecond=1000,
        probeDelaySeconds=0.4,
        backoffDelaySeconds=1,
        toleratedFaults=3,
        resetFaultsDelaySeconds=1
    )

    plans = []
    plans.append({})
    plan = {
        0.6: 2,
        1: 2,
        2: 1,
        2.3: 1,
        2.6: 1,
        3: 1,
        5: 2,
        6: 2
    }
    plans.append(plan)
    plan = {
        1: 0x100,
        2: 0x100,
        3: 0x100,
        4: 0x1000
    }
    plans.append(plan)
    for plan in plans:
        print("[+] testing new plan", plan)
        anyio.run(main, params1, plan)
        print("")
