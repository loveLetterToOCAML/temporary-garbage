

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

