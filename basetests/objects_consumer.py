from baseimplems.datastreams.stream_event import StreamEvent

from anyio import create_memory_object_stream, create_task_group

from contextlib import asynccontextmanager
from typing import Callable, Awaitable


@asynccontextmanager
async def stream_event_consumer(consumer_function: Callable[[StreamEvent], Awaitable[None]], max_buffer_size=0x100, is_sync: bool = False):
    remote_send_stream, local_receive_stream = create_memory_object_stream[StreamEvent](max_buffer_size=max_buffer_size)
    async with create_task_group() as tg:
        async def consumer():
            async with local_receive_stream:
                async for data in local_receive_stream:
                    if is_sync:
                        consumer_function(data)
                    else:
                        await consumer_function(data)
        tg.start_soon(consumer)
        async with remote_send_stream:
            yield remote_send_stream
