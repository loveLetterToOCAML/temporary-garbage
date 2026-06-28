"""
Compression algorithm wrappers with full Pydantic parameter models.

Algorithms:
  - GzipCompressor   — balanced (gzip / zlib, stdlib)
  - LZ4Compressor    — optimised for speed   (lz4 package)
  - ZstdCompressor   — optimised for ratio   (zstandard package)
"""

from __future__ import annotations

import gzip
import time
import zlib
from contextlib import asynccontextmanager

from anyio import AsyncContextManagerMixin
from pydantic import BaseModel, Field

from baseimplems.anyio_utils import NotInAsyncContextManager
from basetypes.implementation.dataformat.compression_protocols import StreamCompressorProtocol, \
    StreamDecompressorProtocol


class InternalGzipCompressionParameters(BaseModel):
    compressionLevel: int = Field(
        default=6,
        ge=0, le=9,
        description='zlib compression level (0=none ... 9=max). Default to 6',
    )
    wbits: int = Field(
        default=31,
        description=(
            'Window size bits for zlib.compressobj. gzip format=31, zlib format=15, raw deflate=-15'
        ),
    )
    memLevel: int = Field(
        default=8,
        ge=1, le=9,
        description='zlib internal memory usage (1=min ... 9=max). Default to 8',
    )
    strategy: int = Field(
        default=0,
        description=(
            'zlib strategy: Z_DEFAULT_STRATEGY=0, Z_FILTERED=1, Z_HUFFMAN_ONLY=2, Z_RLE=3, Z_FIXED=4'
        ),
    )


class Gzip(AsyncContextManagerMixin):
    """Gzip compressor / decompressor — balanced speed / ratio using Python stdlib."""

    def __init__(self, params):
        self._compress_obj = self._decompress_obj = None
        self._params = params
        self._wbits = -15

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
    from basetypes.implementation.dataformat.compression import DefaultCompressionParameters
    import anyio

    async def perform():
        g = GzipCompressor(DefaultCompressionParameters())
        async with g:
            d1 = g.compress(b'andiaolzopammalp')
            print(d1)
            d2 = g.compress_and_flush(b'pppalsmla')
            print(d2)
            print(d1+d2)

        g = GzipDecompressor(DefaultCompressionParameters())
        async with g:
            d1 = g.decompress(d2[:0x8])
            print(d1)
            d2 = g.decompress_and_flush(d2[0x8:])
            print(d2)

    anyio.run(perform)
