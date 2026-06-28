from basetypes.implementation.dataformat.compression_protocols import StreamCompressorProtocol, \
    StreamDecompressorProtocol
from basetypes.implementation.dataformat.compression import LZ4CompressionParameters
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin
import lz4.frame

from contextlib import asynccontextmanager


class Lz4(AsyncContextManagerMixin):
    """Lz4 compressor / decompressor"""

    def __init__(self, params: LZ4CompressionParameters):
        self._compress_obj = self._decompress_obj = None
        self._params = params

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._compress_obj = lz4.frame.LZ4FrameCompressor(
            compression_level=self._params.compressionLevel,
            block_linked=self._params.withHistory,
            block_size=self._params.blockSize,
            block_checksum=self._params.withChecksum,
        )
        self._decompress_obj = lz4.frame.LZ4FrameDecompressor()
        yield self

    def begin_frame(self):
        if not self._compress_obj:
            raise NotInAsyncContextManager('begin_frame', 'Lz4')
        return self._compress_obj.begin()

    def compress(self, data: bytes) -> bytes:
        if not self._compress_obj:
            raise NotInAsyncContextManager('compress', 'Lz4')
        return self._compress_obj.compress(data)

    def compress_and_flush(self, data: bytes = b'') -> bytes:
        return self.compress(data) + self._compress_obj.flush()

    def decompress(self, data: bytes) -> bytes:
        if not self._decompress_obj:
            raise NotInAsyncContextManager('decompress', 'Lz4')
        return self._decompress_obj.decompress(data)

    def decompress_and_flush(self, data: bytes = b'') -> bytes:
        return self.decompress(data)


class Lz4Compressor(Lz4, StreamCompressorProtocol):
    begin = Lz4.begin_frame
    end = Lz4.compress_and_flush

class Lz4Decompressor(Lz4, StreamDecompressorProtocol):
    end = Lz4.decompress_and_flush


if __name__ == '__main__':
    import anyio

    async def perform():
        g = Lz4Compressor(LZ4CompressionParameters(withHistory=True))
        async with g:
            d1 = g.begin() + g.compress(b'andiaolzopammalp')
            print(d1)
            d2 = g.compress_and_flush(b'pppalsmla')
            print(d2)
            d = d1+d2
            print(d)

        g = Lz4Decompressor(LZ4CompressionParameters(withHistory=True))
        async with g:
            d1 = g.decompress(d[:0x8])
            print(d1)
            d2 = g.decompress_and_flush(d[0x8:])
            print(d2)

    anyio.run(perform)
