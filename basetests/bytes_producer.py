from baseimplems.datastreams.event_collector import next_stream_event_collector, current_stream_event_stream, \
    stream_event_collector
from baseimplems.datastreams.stream_event import TransitStatus

from contextlib import asynccontextmanager
from functools import wraps
from random import randint
from enum import Enum


def within_new_bytes_event_stream(f):
    @wraps(f)
    async def res(*args, **kwargs):
        async with next_stream_event_collector(f.__name__):
            offset = 0
            async for data in f(*args, **kwargs):
                yield data
                await current_stream_event_stream.get().new_bytes_event(offset, len(data), status=TransitStatus.SENDING)
                offset += len(data)
    return res


@within_new_bytes_event_stream
async def create_random_stream(data_size=1000000000, max_bound=0x10000):
    ctr = 0
    while ctr < data_size:
        sz = randint(1, max_bound)
        data = bytes([randint(0, 255)] * sz)
        ctr += len(data)
        to_yield = data[:len(data) - ctr + data_size] if ctr > data_size else data
        yield to_yield


@within_new_bytes_event_stream
async def create_big_fixed_stream(data_size=1000000000, fixed_n=3, send_by_chunks_of=0x400):
    ctr = 0
    cur = 0
    data = b''
    while ctr < data_size:
        data += bytes([cur, (cur+1)%0x100, (cur*2)%0x100] * fixed_n)
        cur = (cur*cur*cur + cur*3 + 3 + cur*cur*7) % 0x100
        ctr += fixed_n * 3
        if ctr >= data_size:
            data = data[:len(data) - ctr + data_size]
            yield data
            return

        while len(data) > send_by_chunks_of:
            yield data[:send_by_chunks_of]
            data = data[send_by_chunks_of:]


class StreamType(Enum):
    RANDOM = 1
    FIXED = 2

async def produce_test_data(stream_type: StreamType, data_size=1000000, *args):
    if stream_type is StreamType.RANDOM:
        f = create_random_stream
    elif stream_type is StreamType.FIXED:
        f = create_big_fixed_stream
    else:
        raise NotImplementedError

    async for bytes_chunk in f(data_size, *args):
        yield bytes_chunk



@within_new_bytes_event_stream
async def randomly_sleep(gen, proba_sleep=10, sleep_delay=1):
    async for data in gen():
        yield data
        if randint(0, proba_sleep) == 0:
            await sleep(sleep_delay)


@asynccontextmanager
async def bytes_generator_to_stream(gen, max_buffer_size=0x1000):
    local_send_stream, remote_receive_stream = create_memory_object_stream[bytes](max_buffer_size=max_buffer_size)
    async with create_task_group() as tg:
        async def producer():
            async with local_send_stream:
                async for data in gen():
                    await local_send_stream.send(data)
        tg.start_soon(producer)
        yield remote_receive_stream


if __name__ == '__main__':
    from anyio import run, sleep, create_memory_object_stream, create_task_group
    from hashlib import sha256, sha512


    async def per_f(f, *args):
        h1 = sha256()
        h2 = sha512()
        async for chunk in f(*args):
            h1.update(chunk)
            h2.update(chunk)
        print("sha256", h1.hexdigest())
        print("sha512", h2.hexdigest())

    async def main():
        async with (
            stream_event_collector.get(),
            create_task_group() as tg,
        ):
            tg.start_soon(per_f, produce_test_data, StreamType.RANDOM)
            tg.start_soon(per_f, produce_test_data, StreamType.FIXED)

    run(main)
