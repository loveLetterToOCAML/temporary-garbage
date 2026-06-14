from basetypes.implementation.dataformat.compression import LZ4Mode

from anyio import to_thread
import lz4.frame
import anyio

"""
block_size=BLOCKSIZE_DEFAULT,
block_linked=True,
compression_level=COMPRESSIONLEVEL_MIN,
content_checksum=False,
block_checksum=False,
auto_flush=False,
return_bytearray=False):
"""

import lz4.frame
import anyio
from anyio import to_thread


async def lz4_streaming_compressor(
        source: anyio.abc.ObjectReceiveStream[bytes],
        reset_recv: anyio.abc.ObjectReceiveStream,  # peer sends a value here to signal bad state
        block_send: anyio.abc.ObjectSendStream[bytes],
        chunk_size: int = 64 * 1024,
):
    """
    Compresses source into block_send using lz4 with block_linked=True.
    When a value arrives on reset_recv, the current frame is closed cleanly
    and a new compressor is started â€” the peer can resync from the next block.
    """
    limiter = anyio.CapacityLimiter(1)
    buf = bytearray()

    def new_compressor():
        c = lz4.frame.LZ4FrameCompressor(block_linked=True)
        return c, c.begin()

    async def compress_and_send(compressor, chunk: bytes) -> None:
        compressed = await to_thread.run_sync(compressor.compress, chunk, limiter=limiter)
        await block_send.send(compressed)

    async def close_frame(compressor) -> None:
        tail = await to_thread.run_sync(compressor.end, limiter=limiter)
        await block_send.send(tail)

    async with block_send, source, reset_recv:
        compressor, header = await to_thread.run_sync(new_compressor, limiter=limiter)
        await block_send.send(header)

        async for raw in source:
            buf += raw

            while len(buf) >= chunk_size:
                chunk = bytes(buf[:chunk_size])
                buf = buf[chunk_size:]

                reset = False
                with anyio.CancelScope(deadline=0):
                    await reset_recv.receive()
                    reset = True

                if reset:
                    await close_frame(compressor)
                    compressor, header = await to_thread.run_sync(new_compressor, limiter=limiter)
                    await block_send.send(header)

                await compress_and_send(compressor, chunk)

        if buf:
            await compress_and_send(compressor, bytes(buf))
        await close_frame(compressor)


class LZ4StreamCompressor:

    def __init__(
            self,
            compression_level: int = 0,  # 0 = fast mode (LZ4), 9-16 = LZ4HC
            block_linked: bool = True,  # history across blocks â€” key for adaptation
            store_size: bool = False,  # don't store uncompressed size in frame header
    ) -> None:
        self._compressor = lz4.frame.LZ4FrameCompressor(
            compression_level=compression_level,
            block_linked=block_linked,
            store_size=store_size,
        )
        self._limiter = anyio.CapacityLimiter(1)
        self._started = False

    def _sync_begin(self) -> bytes:
        self._started = True
        return self._compressor.begin()  # emits the frame header

    def _sync_compress(self, chunk: bytes) -> bytes:
        return self._compressor.compress(chunk)

    def _sync_flush(self) -> bytes:
        return self._compressor.flush()  # flushes without ending the frame

    def _sync_end(self) -> bytes:
        self._started = False
        return self._compressor.end()  # emits end mark + checksum

    async def begin(self) -> bytes:
        return await to_thread.run_sync(self._sync_begin, limiter=self._limiter)

    async def compress(self, chunk: bytes) -> bytes:
        return await to_thread.run_sync(self._sync_compress, chunk, limiter=self._limiter)

    async def flush(self) -> bytes:
        return await to_thread.run_sync(self._sync_flush, limiter=self._limiter)

    async def end(self) -> bytes:
        return await to_thread.run_sync(self._sync_end, limiter=self._limiter)


class LZ4Compressor(BaseModel):
    """LZ4 compressor â€” maximises throughput (speed-first)."""
    params: LZ4Params = Field(default_factory=LZ4Params)

    def compress(self, data: bytes) -> tuple[bytes, float]:
        t0 = time.perf_counter()
        if self.params.mode == LZ4Mode.FRAME:
            compressed = lz4.frame.compress(
                data,
                compression_level=self.params.compression_level,
                block_size=self.params.block_size,
                content_checksum=self.params.content_checksum,
            )
        else:
            compressed = lz4.block.compress(
                data,
                acceleration=self.params.acceleration,
                store_size=self.params.store_size,
            )
        elapsed = (time.perf_counter() - t0) * 1000
        return compressed, elapsed

    def decompress(self, data: bytes, original_size: int = 0) -> tuple[bytes, float]:
        t0 = time.perf_counter()
        if self.params.mode == LZ4Mode.FRAME:
            result = lz4.frame.decompress(data)
        else:
            result = lz4.block.decompress(
                data,
                uncompressed_size=original_size if not self.params.store_size else 0,
            )
        elapsed = (time.perf_counter() - t0) * 1000
        return result, elapsed

    def run(self, data: bytes) -> CompressionResult:
        compressed, c_ms = self.compress(data)
        decompressed, d_ms = self.decompress(compressed, original_size=len(data))
        orig = len(data)
        comp = len(compressed)
        label = f"lz4-{'frame' if self.params.mode == LZ4Mode.FRAME else 'block'}"
        if self.params.compression_level > 0:
            label += f"-hc{self.params.compression_level}"
        return CompressionResult(
            algorithm=label,
            original_size=orig,
            compressed_size=comp,
            compression_ratio=round(orig / comp, 4) if comp else float("inf"),
            space_saving_pct=round((1 - comp / orig) * 100, 2) if orig else 0.0,
            compress_time_ms=round(c_ms, 4),
            decompress_time_ms=round(d_ms, 4),
            lossless_verified=decompressed == data,
        )