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
                                           max_per_second: float = 10.0, max_events_retained: int = 10, is_sync: bool = False):
    remote_send_stream, local_receive_stream = create_memory_object_stream[StreamEvent](max_buffer_size=1)

    def new_sliding_window():
        return [None for _ in range(max_events_retained)]

    async with create_task_group() as tg:
        async def consumer():
            sliding_window = {}
            cursor = {}
            print_at = {}

            async with local_receive_stream:
                it = local_receive_stream.__aiter__()
                while True:
                    with move_on_after(1.0 / max_per_second) as _:
                        while True:
                            try:
                                data: StreamEvent = await it.__anext__()
                            except StopAsyncIteration:  # async for exhaustion
                                return

                            k = hash(data)
                            cursor.setdefault(k, 0)
                            print_at.setdefault(k, 0)
                            sliding_window.setdefault(k, new_sliding_window())
                            sliding_window[k][cursor[k] % max_events_retained] = data
                            cursor[k] += 1
                            if cursor[k] % max_events_retained == print_at[k] % max_events_retained:
                                print_at[k] += 1

                    for key in sliding_window:
                        if not sliding_window[key][print_at[key] % max_events_retained]:
                            continue

                        if cursor[key] % max_events_retained != print_at[key] % max_events_retained:
                            if is_sync:
                                consumer_function(sliding_window[key][print_at[key] % max_events_retained])
                            else:
                                await consumer_function(sliding_window[key][print_at[key] % max_events_retained])
                            print_at[key] += 1

        tg.start_soon(consumer)
        yield remote_send_stream
