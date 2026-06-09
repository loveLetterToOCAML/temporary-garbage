"""
Compression algorithm wrappers with full Pydantic parameter models.

Algorithms:
  - GzipCompressor   — balanced (gzip / zlib, stdlib)
  - LZ4Compressor    — optimised for speed   (lz4 package)
  - ZstdCompressor   — optimised for ratio   (zstandard package)
"""

from __future__ import annotations

import gzip
import lz4.frame
import lz4.block
import zstandard as zstd
import time
import zlib
from enum import IntEnum
from typing import Literal, Optional
from pydantic import BaseModel, Field, model_validator




class GzipCompressor(BaseModel):
    """Gzip compressor — balanced speed / ratio using Python stdlib."""
    params: GzipParams = Field(default_factory=GzipParams)

    model_config = {"arbitrary_types_allowed": True}

    def compress(self, data: bytes) -> tuple[bytes, float]:
        obj = zlib.compressobj(
            self.params.compresslevel,
            zlib.DEFLATED,
            self.params.wbits,
            self.params.memLevel,
            self.params.strategy,
        )
        t0 = time.perf_counter()
        compressed = obj.compress(data) + obj.flush()
        elapsed = (time.perf_counter() - t0) * 1000
        return compressed, elapsed

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




class ZstdCompressor(BaseModel):
    """Zstandard compressor — maximises compression ratio."""
    params: ZstdParams = Field(default_factory=ZstdParams)

    model_config = {"arbitrary_types_allowed": True}

    def _build_compressor(self) -> zstd.ZstdCompressor:
        advanced_kwargs = {
            k: getattr(self.params, k)
            for k in ("window_log", "chain_log", "hash_log",
                      "search_log", "min_match", "target_length", "strategy")
            if getattr(self.params, k) is not None
        }
        if advanced_kwargs:
            # map strategy enum → int
            if "strategy" in advanced_kwargs:
                advanced_kwargs["strategy"] = int(advanced_kwargs["strategy"])
            params_obj = zstd.ZstdCompressionParameters.from_level(
                self.params.level, **advanced_kwargs
            )
            return zstd.ZstdCompressor(
                compression_params=params_obj,
                threads=self.params.threads,
                write_checksum=self.params.write_checksum,
                write_content_size=self.params.write_content_size,
            )
        return zstd.ZstdCompressor(
            level=self.params.level,
            threads=self.params.threads,
            write_checksum=self.params.write_checksum,
            write_content_size=self.params.write_content_size,
        )

    def compress(self, data: bytes) -> tuple[bytes, float]:
        c = self._build_compressor()
        t0 = time.perf_counter()
        compressed = c.compress(data)
        elapsed = (time.perf_counter() - t0) * 1000
        return compressed, elapsed

    def decompress(self, data: bytes) -> tuple[bytes, float]:
        d = zstd.ZstdDecompressor()
        t0 = time.perf_counter()
        result = d.decompress(data)
        elapsed = (time.perf_counter() - t0) * 1000
        return result, elapsed

    def run(self, data: bytes) -> CompressionResult:
        compressed, c_ms = self.compress(data)
        decompressed, d_ms = self.decompress(compressed)
        orig = len(data)
        comp = len(compressed)
        return CompressionResult(
            algorithm=f"zstd (level={self.params.level})",
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