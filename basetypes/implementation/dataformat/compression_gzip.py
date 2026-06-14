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
from pydantic import BaseModel, Field

from basetypes.implementation.dataformat.compression import CompressionResult
from basetypes.implementation.dataformat.compression_protocols import StreamCompressorProtocol


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



class GzipCompressor(StreamCompressorProtocol):
    """Gzip compressor — balanced speed / ratio using Python stdlib."""

    def __init__(self, params):
        self.params = params

    def compress(self, data: bytes) -> bytes:
        obj = zlib.compressobj(
            self.params.compressionLevel,
            zlib.DEFLATED,
            -15,
            8,
            0,
        )
        t0 = time.perf_counter()
        compressed = obj.compress(data) + obj.flush()
        elapsed = (time.perf_counter() - t0) * 1000
        return compressed

    def decompress(self, data: bytes) -> tuple[bytes, float]:
        t0 = time.perf_counter()
        result = zlib.decompress(data, self.params.wbits)
        elapsed = (time.perf_counter() - t0) * 1000
        return result, elapsed

    def run(self, data: bytes) -> CompressionResult:
        compressed, c_ms = self.compress(data)
        decompressed, d_ms = self.decompress(compressed)
        orig = len(data)
        comp = len(compressed)
        return CompressionResult(
            algorithm=f"gzip (level={self.params.compresslevel})",
            original_size=orig,
            compressed_size=comp,
            compression_ratio=round(orig / comp, 4) if comp else float("inf"),
            space_saving_pct=round((1 - comp / orig) * 100, 2) if orig else 0.0,
            compress_time_ms=round(c_ms, 4),
            decompress_time_ms=round(d_ms, 4),
            lossless_verified=decompressed == data,
        )



# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json, textwrap

    # Generate a compressible payload (~200 KB of repetitive JSON-like text)
    payload = (
        '{"id": %d, "name": "sample record", "value": 3.14159, '
        '"active": true, "tags": ["foo", "bar", "baz"]}\n'
    ) * 3_000
    data = payload.encode()

    print(f"Payload size: {len(data):,} bytes\n{'─'*65}")

    compressors = [
        GzipCompressor(params=GzipParams(compresslevel=6)),
        LZ4Compressor(params=LZ4Params(mode=LZ4Mode.FRAME, compression_level=0)),
        ZstdCompressor(params=ZstdParams(level=19)),
    ]

    results: list[CompressionResult] = []
    for c in compressors:
        r = c.run(data)
        results.append(r)
        print(r)

    # Pydantic serialisation demo
    print(f"\n{'─'*65}")
    print("JSON serialisation of the first result (GzipCompressor):\n")
    print(textwrap.indent(results[0].model_dump_json(indent=2), "  "))

    print(f"\n{'─'*65}")
    print("JSON serialisation of GzipParams:\n")
    gzip_params = GzipParams(compresslevel=9)
    print(textwrap.indent(gzip_params.model_dump_json(indent=2), "  "))

    print(f"\n{'─'*65}")
    print("Deserialise ZstdParams from dict:\n")
    raw = {"level": 15, "threads": 2, "write_checksum": False,
           "strategy": ZstdStrategy.BTULTRA2}
    zp = ZstdParams(**raw)
    print(textwrap.indent(zp.model_dump_json(indent=2), "  "))