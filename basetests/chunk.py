import random
from contextlib import contextmanager, asynccontextmanager
from random import randint
from typing import Generator, Self

from anyio import create_task_group, create_memory_object_stream, run, sleep, Semaphore
from anyio.abc import ObjectReceiveStream

from baseimplems.dataformat.chunk import ChunkedBytes


async def create_random_stream():
    ctr = 0
    while ctr < 1000000000:
        sz = randint(1, 0x1000)
        data = bytes([randint(0, 255)] * sz)
        ctr += len(data)
        yield data


async def process_items(receive_stream: ObjectReceiveStream[bytes]) -> None:
    async with receive_stream:
        async for item in receive_stream:
            print('received', len(item))
            await sleep(3)


async def initial_generator():
    local_send_stream, remote_receive_stream = create_memory_object_stream[bytes](0x10)
    async with create_task_group() as tg:
        tg.start_soon(process_items, remote_receive_stream)
        async with local_send_stream:
            async for data in create_random_stream():
                print('send', len(data))
                await local_send_stream.send(data)


@asynccontextmanager
async def produce_1Go_not_random_stream():
    local_send_stream, remote_receive_stream = create_memory_object_stream[bytes]()
    async with create_task_group() as tg:
        async def producer():
            async with local_send_stream:
                async for data in create_random_stream():
                    print('send', len(data))
                    await local_send_stream.send(data)
                    if random.randint(0, 100) == 40:
                        print("Will sleep little to test auto flush")
                        await sleep(1)
                        print("Slept a bit")

        tg.start_soon(producer)
        yield remote_receive_stream


async def main():
    async with produce_1Go_not_random_stream() as process_data:
        cktor = ChunkedBytes(process_data)
        async with cktor as chunk_streamed:
            await process_items(chunk_streamed)


if __name__ == '__main__':
    #chunk_pipeline = raw_bytes_to_chunks()
    run(main)
