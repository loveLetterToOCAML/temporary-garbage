from basetypes.implementation.dataformat.compression import compression_obj_for, CompressionAlgorithmInstance, \
    CommonDataBufferSyncProcessing
from baseimplems.datastreams.chunk import ChunkInMemoryConstraint, ChunkedBytes

from pydantic import BaseModel

from anyio import AsyncContextManagerMixin, create_memory_object_stream, to_thread, move_on_after, EndOfStream
from anyio.abc import ObjectReceiveStream
from contextlib import asynccontextmanager
from typing import Callable
import anyio


class CommonDataBufferAsyncProcessing(AsyncContextManagerMixin):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], sync_process_factory: Callable[[BaseModel], CommonDataBufferSyncProcessing],
                 parameters_for_sync_obj: BaseModel, *, memory_constraints: ChunkInMemoryConstraint | None = None,
                 reset_stream: ObjectReceiveStream | None = None):
        self._memory_constraints = memory_constraints or ChunkInMemoryConstraint()
        self._upper_stream = upper_stream
        self._reset_stream = reset_stream
        self._sync_process_factory = sync_process_factory
        self._parameters_for_sync_obj = parameters_for_sync_obj

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        compressed_data_send, compressed_data_stream = create_memory_object_stream[bytes](
            max_buffer_size=self._memory_constraints.chunkSize
        )
        async with (
            ChunkedBytes(compressed_data_stream, self._memory_constraints) as chunk_stream,
            anyio.create_task_group() as tg
        ):
            compressed_data = bytearray()
            limiter = anyio.CapacityLimiter(1)

            async def reset_and_send():
                compressor: CommonDataBufferSyncProcessing = self._sync_process_factory(self._parameters_for_sync_obj)
                header = await to_thread.run_sync(compressor.begin, limiter=limiter) or b''
                await compressed_data_send.send(header)
                return compressor

            async def compress(compressor, chunk: bytes) -> None:
                nonlocal compressed_data
                compressed_data += await to_thread.run_sync(compressor, chunk, limiter=limiter)
                print("after compression", len(compressed_data))

            async def close_frame(compressor) -> None:
                nonlocal compressed_data
                compressed_data += await to_thread.run_sync(compressor.end, limiter=limiter) or b''

            async def send_current(data):
                await compressed_data_send.send(data)

            async def compressor_main(reentrant: bool = False):
                nonlocal compressed_data
                if self._reset_stream and not reentrant:
                    async with self._reset_stream:
                        return compressor_main(True)

                async with (
                    compressed_data_send,
                    self._upper_stream as chunks,
                ):
                    compressor = await reset_and_send()

                    it = chunks.__aiter__()
                    while True:
                        if self._reset_stream:
                            reset = False
                            with anyio.CancelScope(deadline=0):
                                await self._reset_stream.receive()
                                reset = True

                            if reset:
                                print("Received signal to reset compressor, redoing")
                                await reset_and_send()

                        raw = b''
                        with move_on_after(self._memory_constraints.timeBeforeFlush) as data_or_flush:
                            try:
                                raw = await it.__anext__()
                            except StopAsyncIteration:  # async for exhaustion
                                break

                        print("Did we get raw?", len(raw))
                        if raw:
                            await compress(compressor, raw)

                        while len(compressed_data) >= self._memory_constraints.chunkSize:
                            print("compressed buf is full", len(compressed_data))
                            await send_current(compressed_data[:self._memory_constraints.chunkSize])
                            compressed_data = compressed_data[self._memory_constraints.chunkSize:]

                        if data_or_flush.cancelled_caught and compressed_data:
                            print("sending compressed data", len(compressed_data))
                            await send_current(compressed_data)
                            compressed_data = bytearray()

                    if compressed_data:
                        await close_frame(compressor)
                        await send_current(compressed_data)

            tg.start_soon(compressor_main)
            yield chunk_stream


class CompressedBytes(CommonDataBufferAsyncProcessing):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], compression_alg: CompressionAlgorithmInstance, *,
                 memory_constraints: ChunkInMemoryConstraint | None = None, reset_stream: ObjectReceiveStream | None = None):
        super().__init__(upper_stream, compression_obj_for, compression_alg,
                         memory_constraints=memory_constraints, reset_stream=reset_stream)


class DecompressedBytes(CommonDataBufferAsyncProcessing):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], compression_alg: CompressionAlgorithmInstance, *,
                 memory_constraints: ChunkInMemoryConstraint | None = None,
                 reset_stream: ObjectReceiveStream | None = None):
        super().__init__(upper_stream, compression_obj_for, compression_alg,
                         memory_constraints=memory_constraints, reset_stream=reset_stream)


if __name__ == "__main__":
    from basetypes.implementation.dataformat.compression import  CompressionAlgorithm, GzipCompressionParameters
    from basetests.chunk import produce_1Go_not_random_stream, process_items

    calg = CompressionAlgorithmInstance(
        type=CompressionAlgorithm.GZIP,
        compressionParameters=GzipCompressionParameters()
    )

    async def main():
        async with produce_1Go_not_random_stream(100000) as data_generation:
            cktor = ChunkedBytes(data_generation)
            async with (
                CompressedBytes(cktor, calg) as compressed_chunks,
                FakeNetworkTransmission(compressed_chunks) as net,
                DecompressedBytes(net, calg) as decompressed_chunks
            ):
                await process_items(decompressed_chunks)

    anyio.run(main)