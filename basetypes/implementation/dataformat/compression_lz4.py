from basetypes.implementation.dataformat.compression import LZ4Mode


class LZ4Compressor(BaseModel):
    """LZ4 compressor — maximises throughput (speed-first)."""
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
