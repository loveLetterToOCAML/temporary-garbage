from __future__ import annotations

from basetypes.implementation.dataformat.compression_protocols import StreamCompressorProtocol, \
    StreamDecompressorProtocol
from basetypes.implementation.dataformat.compression import GzipCompressionParameters
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin

from contextlib import asynccontextmanager
import zlib


#class InternalGzipCompressionParameters(BaseModel):
#    compressionLevel: int = Field(
#        default=6,
#        ge=0, le=9,
#        description='zlib compression level (0=none ... 9=max). Default to 6',
#    )
#    wbits: int = Field(
#        default=31,
#        description=(
#            'Window size bits for zlib.compressobj. gzip format=31, zlib format=15, raw deflate=-15'
#        ),
#    )
#    memLevel: int = Field(
#        default=8,
#        ge=1, le=9,
#        description='zlib internal memory usage (1=min ... 9=max). Default to 8',
#    )
#    strategy: int = Field(
#        default=0,
#        description=(
#            'zlib strategy: Z_DEFAULT_STRATEGY=0, Z_FILTERED=1, Z_HUFFMAN_ONLY=2, Z_RLE=3, Z_FIXED=4'
#        ),
#    )


class Gzip(AsyncContextManagerMixin):
    """Gzip compressor / decompressor — balanced speed / ratio using Python stdlib."""

    def __init__(self, params: GzipCompressionParameters):
        self._compress_obj = self._decompress_obj = None
        self._params = params
        self._wbits = -15  # raw deflate

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._compress_obj = zlib.compressobj(
            self._params.compressionLevel,
            zlib.DEFLATED,
            self._wbits,
            8,
            0,
        )
        self._decompress_obj = zlib.decompressobj(
            self._wbits,
        )
        yield self

    def compress(self, data: bytes) -> bytes:
        if not self._compress_obj:
            raise NotInAsyncContextManager('compress', 'Gzip')
        return self._compress_obj.compress(data)

    def compress_and_flush(self, data: bytes = b'') -> bytes:
        return self.compress(data) + self._compress_obj.flush()

    def decompress(self, data: bytes) -> bytes:
        if not self._decompress_obj:
            raise NotInAsyncContextManager('decompress', 'Gzip')
        return self._decompress_obj.decompress(data)

    def decompress_and_flush(self, data: bytes = b'') -> bytes:
        return self.decompress(data) + self._decompress_obj.flush()


class GzipCompressor(Gzip, StreamCompressorProtocol):
    end = Gzip.compress_and_flush

class GzipDecompressor(Gzip, StreamDecompressorProtocol):
    end = Gzip.decompress_and_flush


if __name__ == '__main__':
    import random
    import string
    import anyio

    async def perform():
        g = GzipCompressor(GzipCompressionParameters())
        async with g:
            d1 = g.compress(b'andiaolzopammalp')
            print(d1)
            d2 = g.compress_and_flush(b'pppalsmla')
            print(d2)
            print(d1+d2)

        g = GzipDecompressor(GzipCompressionParameters())
        async with g:
            d1 = g.decompress(d2[:0x8])
            print(d1)
            d2 = g.decompress_and_flush(d2[0x8:])
            print(d2)

        g = GzipCompressor(GzipCompressionParameters())
        async with g:
            for i in range(0x100):
                t = bytes(map(ord, random.choices(string.ascii_letters, k=0x10000)))
                d = g.compress(t)
                if d:
                    print("GOT D", i, len(d))
            print("final D", len(g.compress_and_flush(t)))

    anyio.run(perform)
