import random
from contextlib import contextmanager, asynccontextmanager
from random import randint
from typing import Generator, Self

from anyio import create_task_group, create_memory_object_stream, run, sleep, Semaphore, EndOfStream
from anyio.abc import ObjectReceiveStream

from baseimplems.datastreams.chunk import ChunkedBytes



async def process_items(receive_stream: ObjectReceiveStream[bytes]) -> None:
    async with receive_stream:
        async for item in receive_stream:
            print('received', len(item))
            #await sleep(3)




async def main():
    async with produce_1Go_not_random_stream() as process_data:
        cktor = ChunkedBytes(process_data)
        async with cktor as chunk_streamed:
            await process_items(chunk_streamed)


if __name__ == '__main__':
    #chunk_pipeline = raw_bytes_to_chunks()
    run(main)
