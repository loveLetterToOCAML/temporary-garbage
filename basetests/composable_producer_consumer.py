from baseimplems.datastreams.event_collector import next_stream_event_collector
from basetypes.implementation.dataformat.chunk import DataChunk
from baseimplems.datastreams.stream_event import TransitStatus
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, create_memory_object_stream, create_task_group
from anyio.abc import ObjectReceiveStream

from typing import TypeVar, Generic, AsyncIterator
from contextlib import asynccontextmanager, AsyncExitStack

T = TypeVar('T')
U = TypeVar('U')


class MeasurableStream(AsyncContextManagerMixin, Generic[T, U]):
    """ This stream encapsulates an upper and a lower streams and send events on receive / send
    """

    def __init__(self, upper_stream: ObjectReceiveStream[T]):
        self._local_send_stream = self._remote_receive_stream = None
        self._upper_stream = upper_stream
        self._event_collector = None


    async def _process_data_and_send_event(self, data, status: TransitStatus):
        print("PROCESS AND SEND", len(data))
        if not self._event_collector:
            raise NotInAsyncContextManager('_process_data_and_send_event', 'MeasurableStream')
        if isinstance(data, bytes):
            await self._event_collector.new_bytes_event(self._offset_bytes, len(data), status=status)
            self._offset_bytes += len(data)
        elif isinstance(data, DataChunk):
            await self._event_collector.new_chunk_event(data.index, data.offset, data.size, status=status)
        else:
            await self._event_collector.new_object_event(self._offset_objects, type(data), status=status)
            self._offset_objects += 1


    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[ObjectReceiveStream[T]]:
        self._local_send_stream, self._remote_receive_stream = create_memory_object_stream[T](0x1000)
        self._offset_bytes = 0
        self._offset_objects = 0

        async def producer():
            async with (
                next_stream_event_collector('MeasurableStream') as self._event_collector,
                self._upper_stream,
                self._local_send_stream,
            ):
                async for data in self._upper_stream:
                    await self._local_send_stream.send(data)
                    await self._process_data_and_send_event(data, TransitStatus.QUEUED)

        async with create_task_group() as tg:
            tg.start_soon(producer)
            yield self._remote_receive_stream


    @asynccontextmanager
    async def wire_output(self, encapsulated_out: ObjectReceiveStream[U]) -> AsyncIterator[ObjectReceiveStream[U]]:
        if not self._local_send_stream:
            raise NotInAsyncContextManager('wire_output', 'MeasurableStream')
        local_send_stream, remote_receive_stream = create_memory_object_stream[U](0x1000)

        async def consumer():
            async with (
                encapsulated_out,
                local_send_stream
            ):
                async for data in encapsulated_out:
                    await self._process_data_and_send_event(data, TransitStatus.SENDING)
                    await local_send_stream.send(data)

        async with create_task_group() as tg:
            tg.start_soon(consumer)
            yield remote_receive_stream


async def broadcast_to_sinks(source, *sinks):
    async with source:
        async with create_task_group() as tg:
            async for chunk in source:
                for sink in sinks:
                    tg.start_soon(sink.send, chunk)

    for sink in sinks:
        await sink.aclose()


@asynccontextmanager
async def broadcast_to_n(source, n_receivers: int):
    pairs = [create_memory_object_stream(0x1000) for _ in range(n_receivers)]

    async def main():
        async with (
            source,
            AsyncExitStack() as stack
        ):
            _ = [stack.enter_context(ms_send) for ms_send, _ in pairs]
            async with create_task_group() as chunk_tg:
                async for chunk in source:
                    for ms_send, recv in pairs:
                        chunk_tg.start_soon(ms_send.send, chunk)

    async with create_task_group() as tg:
        tg.start_soon(main)
        yield [recv for _, recv in pairs]
