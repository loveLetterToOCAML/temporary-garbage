from baseimplems.datastreams.stream_event import StreamEvent

from anyio import create_memory_object_stream, create_task_group, move_on_after

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


@asynccontextmanager
async def stream_event_consumer_max_events(consumer_function: Callable[[StreamEvent], Awaitable[None]],
                                           max_per_second: float = 1.0, max_events_retained: int = 10, is_sync: bool = False):
    remote_send_stream, local_receive_stream = create_memory_object_stream[StreamEvent](max_buffer_size=1)

    async with create_task_group() as tg:
        async def consumer():
            sliding_window = [None for _ in range(max_events_retained)]
            cursor = 0
            print_at = 0
            async with local_receive_stream:
                while True:
                    with move_on_after(1.0 / max_per_second) as _:
                        async for data in local_receive_stream:
                            sliding_window[cursor % max_events_retained] = data
                            cursor += 1
                            if cursor % max_events_retained == print_at:
                                print_at += 1

                    if not sliding_window[print_at % max_events_retained]:
                        continue

                    if is_sync:
                        consumer_function(sliding_window[print_at % max_events_retained])
                    else:
                        await consumer_function(sliding_window[print_at % max_events_retained])
                    print_at += 1

        tg.start_soon(consumer)
        yield remote_send_stream
