from baseimplems.datastreams.stream_event import BytesStreamEvent, TransitStatus, base_event_from, StreamEndReason
from baseimplems.datastreams.constrained import StreamConstraints, ConstraintsHandler
from baseimplems.datastreams.observers import StatePoller

from anyio import AsyncContextManagerMixin, create_memory_object_stream, create_task_group, CancelScope, move_on_after
from anyio.abc import ObjectReceiveStream
import anyio

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
import time


@contextmanager
def measure_time_for_block():
    class _Timer:
        elapsed: float = 0.0
    t = _Timer()
    start = time.perf_counter()
    try:
        yield t
    finally:
        t.elapsed = time.perf_counter() - start


current_constrained_sender_index = ContextVar[int]('current_constrained_sender_index', default=0)


class ConstrainedStreamSender(ConstraintsHandler, AsyncContextManagerMixin):

    def __init__(self, remote_constraints: StreamConstraints, upper_stream: ObjectReceiveStream[bytes]):
        idx = current_constrained_sender_index.get()
        current_constrained_sender_index.set(idx + 1)
        super().__init__(remote_constraints, 'bytestream-constrained-sender', idx)

        self._reference_sleep_delay = min(remote_constraints.probeDelaySeconds / 10.0, 0.1)
        self._chunk_size = int(remote_constraints.maxBytesPerSecond * self._reference_sleep_delay)
        if self._chunk_size <= 10:
            raise Exception(f"Computed {self._chunk_size} in ConstrainedStreamSender; this abnormal value should never happen")

        self._upper_stream = upper_stream


    async def _receive_and_transfer(self, cancel_scope: CancelScope, local_constrained_stream_send):
        await anyio.sleep(self._params.bootstrapDelaySeconds)
        self._bootstrapped = time.monotonic()
        self._sleep_delay = self._reference_sleep_delay
        self._cumulated_delay = 0.0
        self._expected_cumulated_delay = 0.0
        buffer = b''
        while True:
            with (
                measure_time_for_block() as real_time_waiting,
                move_on_after(self._sleep_delay) as max_time
            ):
                    no_data = True
                    if len(buffer) > self._chunk_size:
                        no_data = False
                        await anyio.sleep(self._sleep_delay)  # will fall outside the move_on_after without consuming more
                    async for data in self._upper_stream:
                        no_data = False
                        buffer += data  # TODO: we do a copy of data here; may be not appropriated
                        if len(buffer) > self._chunk_size:
                            await anyio.sleep(self._sleep_delay)  # will fall outside the move_on_after without consuming more

            self._cumulated_delay += real_time_waiting.elapsed
            self._expected_cumulated_delay += self._reference_sleep_delay

            if no_data:
                # TODO: logger.info('No data received, ensure throughput is ok')
                pass

            if not max_time.cancelled_caught:
                self._update_end_status(cancel_scope, StreamEndReason.END_OF_INPUT, 'remote peer disconnected')

            stream_event = None
            if buffer:
                sz = min(self._chunk_size, len(buffer))
                with measure_time_for_block() as real_time_waiting_send:
                    await local_constrained_stream_send.send(buffer[:self._chunk_size])
                self._cumulated_delay += real_time_waiting_send.elapsed
                buffer = buffer[self._chunk_size:]
                stream_event = BytesStreamEvent(offset=0, size=sz, status=TransitStatus.SENDING,
                                                **self._stream_identifier, **base_event_from(self._creation_time))
            self._update_stream_stats(stream_event)

            self._sleep_delay = self._reference_sleep_delay + self._expected_cumulated_delay - self._cumulated_delay
            if self._sleep_delay < 0:
                self._sleep_delay = 0


    @asynccontextmanager
    async def __asynccontextmanager__(self):
        local_constrained_stream_send, remote_constrained_stream_receive = create_memory_object_stream[bytes]()
        external_stream_poller = StatePoller(self.current_internal_state)

        async with (
            external_stream_poller,
            create_task_group() as tg,
        ):
            # TODO: avoid these 2 vars as it's implicit contract with ConstraintsHandler
            self._current_stream_status = None
            self._started_at = time.monotonic()
            with CancelScope() as cancel_scope:
                tg.start_soon(self._receive_and_transfer, cancel_scope, local_constrained_stream_send)
                tg.start_soon(self._handle_stream_constraints, cancel_scope)
                tg.start_soon(self._handle_global_timeout, cancel_scope, 'bytestream constrained sender global timeout')
                yield remote_constrained_stream_receive, external_stream_poller

            tg.cancel_scope.cancel()  # cancel task group after any condition, even if we could only print when receiving failure (while remote accepts it's ok)



if __name__ == '__main__':
    from baseimplems.datastreams.constrained import StreamWatchguard, generate_stream_event_task

    from basetests.bytes_producer import create_random_stream, bytes_generator_to_stream


    async def print_all(prefix, stream_poller):
        async for value in stream_poller:
            print(prefix, value)
            await anyio.sleep(1)


    @asynccontextmanager
    async def stream_watchguard(params):
        guard = StreamWatchguard(params)
        async with (
            guard as (remote_send_stream, external_stream_poller, external_stream_end),
            create_task_group() as tg
        ):
            tg.start_soon(print_all, "[+guard] while it works", external_stream_poller)
            yield remote_send_stream
        print("last value", guard.current_internal_state())


    async def main(params):
        stream_identifier = {
            'name': 'stream-mock',
            'index': 0,
            'randomId': 1337,
            **base_event_from()
        }

        async with (
            bytes_generator_to_stream(create_random_stream(max_bound=10000), max_buffer_size=10000) as base_stream,
            base_stream,
            stream_watchguard(params) as stats_control,
            stats_control
        ):
            send = ConstrainedStreamSender(params, base_stream)
            async with (
                send as (remote_send_stream, external_stream_poller),
                remote_send_stream,
                create_task_group() as tg
            ):
                tg.start_soon(print_all, "[+sender] value while it works", external_stream_poller)
                async for data in remote_send_stream:
                    await stats_control.send(BytesStreamEvent(offset=0, size=len(data), status=TransitStatus.DONE, **stream_identifier))
                    print(f"[+] Received {len(data)} data")


    params = StreamConstraints(
        maximumStreamDurationSeconds=400,
        bootstrapDelaySeconds=2,
        minBytesPerSecond=2,
        maxBytesPerSecond=10000,
        probeDelaySeconds=1,
        backoffDelaySeconds=1,
        toleratedFaults=3,
        resetFaultsDelaySeconds=1
    )

    anyio.run(main, params)