from __future__ import annotations

from basetypes.implementation.dataformat.compression_protocols import CommonDataBufferSyncProcessing
from basetypes.implementation.basetypes_match import DefaultBaseType

from pydantic import BaseModel, Field, model_validator

from contextlib import asynccontextmanager
from typing import Literal, Any
from enum import Enum


class CompressionAbortReason(Enum):
    MAX_OUTPUT_EXCEEDED = 1  # raw size ceiling hit for chunk
    RATIO_EXCEEDED = 2  # expansion ratio spike
    TIMEOUT = 3  # wall-clock limit hit
    INPUT_TOO_LARGE = 4  # compressed chunk ceiling
    CORRUPTED_DATA = 5  # compression integrity error


class CompressionAlgorithm(Enum):
    GZIP = 1
    ZSTD = 2
    LZ4 = 3
    ...
    OTHER = 0xff


class CompressionAlgorithmInstance(BaseModel):
    type: CompressionAlgorithm
    compressionParameters: Any


class DefaultCompressionParameters(BaseModel):
    compressionLevel: int = 6
    withHistory: bool = True  # take advantages of dynamic window recomputation through time and emitted blocks
    withChecksum: bool = False  # compute checksum of each block for basic verification, at the cost of being potentially non-standard if not supported by the algorithm


class GzipCompressionParameters(DefaultCompressionParameters):
    compressionLevel: int = Field(default=6, ge=4, le=8)
    # wbits: int  # default to -15 = raw deflate  # as there is a separate checksum per chunk supported elsewhere
    # memLevel: int  # default to 8
    # strategy: int  # default to 0 = Z_DEFAULT_STRATEGY


class Gzip(CompressionAlgorithmInstance):
    type: Literal[CompressionAlgorithm.GZIP] = CompressionAlgorithm.GZIP
    compressionParameters: GzipCompressionParameters


class LZ4CompressionParameters(DefaultCompressionParameters):
    compressionLevel: int = Field(default=3, ge=0, le=16)
    blockSize: int = Field(
        default=0,
        description='Block size (bytes). 0 = library default (~4 MB). Valid non-zero values: 65536, 262144, 1048576, 4194304',
    )

    @model_validator(mode="after")
    def _check_block_size(self) -> LZ4CompressionParameters:
        valid = {0, 65536, 262144, 1048576, 4194304}
        if self.blockSize not in valid:
            raise ValueError(f"block_size must be one of {valid}")
        return self


class Lz4(CompressionAlgorithmInstance):
    type: Literal[CompressionAlgorithm.LZ4] = CompressionAlgorithm.LZ4
    compressionParameters: LZ4CompressionParameters


class ZstdStrategy(Enum):
    FAST = 1
    DFAST = 2
    GREEDY = 3
    LAZY = 4
    LAZY2 = 5
    BTLAZY2 = 6
    BTOPT = 7
    BTULTRA = 8
    BTULTRA2 = 9


class ZstdCompressionParameters(DefaultCompressionParameters):
    level: int = Field(default=3, ge=1, le=22)
    windowLog: int | None = Field(
        default=None,
        ge=10, le=31,
        description='Log2 of the sliding window size. None = auto',
    )
    chainLog: int | None = Field(
        default=None,
        ge=6, le=30,
        description='Log2 of the hash-chain table size (affects ratio). None = auto',
    )
    hashLog: int | None = Field(
        default=None,
        ge=6, le=30,
        description='Log2 of the hash table size. None = auto.',
    )
    searchLog: int | None = Field(
        default=None,
        ge=1, le=30,
        description='Number of search attempts per position. None = auto',
    )
    minMatch: int | None = Field(
        default=None,
        ge=3, le=7,
        description='Minimum match length (bytes). None = auto',
    )
    targetLength: int | None = Field(
        default=None,
        ge=0,
        description='Target match length for strategies BTOPT/BTULTRA/BTULTRA2. None = auto',
    )
    strategy: ZstdStrategy | None = Field(
        default=None,
        description='Internal match-finder strategy. None = determined by level',
    )


class Zstd(CompressionAlgorithmInstance):
    type: Literal[CompressionAlgorithm.ZSTD] = CompressionAlgorithm.ZSTD
    compressionParameters: ZstdCompressionParameters


class CompressedData(BaseModel):
    compressionAlgorithm: CompressionAlgorithmInstance
    compressedBytes: bytes


def compression_obj_for(instance: CompressionAlgorithmInstance) -> CommonDataBufferSyncProcessing:
    match instance.type:
        case CompressionAlgorithm.GZIP:
            from basetypes.implementation.dataformat.compression_gzip import GzipCompressor
            return GzipCompressor(instance.compressionParameters)
        case CompressionAlgorithm.ZSTD:
            pass
        case CompressionAlgorithm.LZ4:
            from basetypes.implementation.dataformat.compression_lz4 import Lz4Compressor
            return Lz4Compressor(instance.compressionParameters)
        case _:
            raise NotImplementedError


def decompression_obj_for(instance: CompressionAlgorithmInstance) -> CommonDataBufferSyncProcessing:
    match instance.type:
        case CompressionAlgorithm.GZIP:
            from basetypes.implementation.dataformat.compression_gzip import GzipDecompressor
            return GzipDecompressor(instance.compressionParameters)
        case CompressionAlgorithm.ZSTD:
            pass
        case CompressionAlgorithm.LZ4:
            from basetypes.implementation.dataformat.compression_lz4 import Lz4Decompressor
            return Lz4Decompressor(instance.compressionParameters)
        case _:
            raise NotImplementedError


@asynccontextmanager
async def async_decompress(instance: CompressionAlgorithmInstance):
    decompressor = compression_obj_for(instance)
    async with decompressor:
        yield decompressor


class CompressionResult(BaseModel):
    algorithm: CompressionAlgorithmInstance
    originalSize: int
    compressedSize: int
    compressionRatio: float
    compressTime: DefaultBaseType.TIMEDELTA | None
    decompressTime: DefaultBaseType.TIMEDELTA | None
