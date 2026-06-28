from basetypes.implementation.dataformat.compression import compression_obj_for, CompressionAlgorithmInstance, \
    decompression_obj_for
from baseimplems.datastreams.chunk import ChunkInMemoryConstraint, ChunkedBytes, CommonDataBufferAsyncProcessing
from basetests.composable_producer_consumer import MeasurableStream, broadcast_to_n

from anyio.abc import ObjectReceiveStream
from anyio import Event
import anyio


class CompressedBytes(CommonDataBufferAsyncProcessing):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], compression_alg: CompressionAlgorithmInstance, *,
                 memory_constraints: ChunkInMemoryConstraint | None = None, reset_event: Event | None = None):
        super().__init__(upper_stream, compression_obj_for, compression_alg,
                         memory_constraints=memory_constraints, reset_event=reset_event)


class DecompressedBytes(CommonDataBufferAsyncProcessing):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], compression_alg: CompressionAlgorithmInstance, *,
                 memory_constraints: ChunkInMemoryConstraint | None = None,
                 reset_event: Event | None = None):
        super().__init__(upper_stream, decompression_obj_for, compression_alg,
                         memory_constraints=memory_constraints, reset_event=reset_event)


if __name__ == "__main__":
    from basetypes.implementation.dataformat.compression import CompressionAlgorithm, LZ4CompressionParameters
    from basetests.bytes_producer import produce_test_data, StreamType, bytes_generator_to_stream
    from baseimplems.datastreams.event_collector import run_with_event_collector, stream_event_collector
    from anyio import create_task_group
    from hashlib import sha256, sha512

    calg = CompressionAlgorithmInstance(
        type=CompressionAlgorithm.LZ4,
        compressionParameters=LZ4CompressionParameters()
    )

    async def compute_hash(stream):
        h1 = sha256()
        h2 = sha512()
        async for chunk in stream:
            h1.update(chunk)
            h2.update(chunk)
        print("sha256", h1.hexdigest())
        print("sha512", h2.hexdigest())

    async def main():
        async with (
            run_with_event_collector(),
            stream_event_collector.get(),
            bytes_generator_to_stream(produce_test_data, StreamType.FIXED) as data_generation
        ):
            async with (
                ChunkedBytes(data_generation) as base_chunks,
                broadcast_to_n(base_chunks, 2) as (clone1, clone2),
                clone1,
                clone2,
                (ms1 := MeasurableStream(clone1)) as ms1_in,
                CompressedBytes(ms1_in, calg) as compressed_chunks,
                ms1.wire_output(compressed_chunks) as ms1_out,
                #FakeNetworkTransmission(compressed_chunks) as net,
                DecompressedBytes(ms1_out, calg) as decompressed_chunks,
                create_task_group() as tg,
            ):
                tg.start_soon(compute_hash, clone2)
                tg.start_soon(compute_hash, decompressed_chunks)

    anyio.run(main)
