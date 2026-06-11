from __future__ import annotations

from basetypes.implementation.basetypes_match import DefaultBaseType

from pydantic import BaseModel, Field, model_validator

from typing import Literal, Any
from enum import Enum



class CompressionAbortReason(Enum):
    MAX_OUTPUT_EXCEEDED  = 1  # raw size ceiling hit for chunk
    RATIO_EXCEEDED       = 2  # expansion ratio spike
    TIMEOUT              = 3  # wall-clock limit hit
    INPUT_TOO_LARGE      = 4  # compressed chunk ceiling
    CORRUPTED_DATA       = 5  # compression integrity error


class CompressionAlgorithm(Enum):
    GZIP = 1
    ZSTD = 2
    LZ4 = 3
    ...
    OTHER = 0xff


class KnownCompressionAlgorithm(BaseModel):
    type: CompressionAlgorithm
    compressionParameters: Any


class GzipCompressionParameters(BaseModel):
    compressionLevel: int = Field(default=6, ge=4, le=8)
    # wbits: int  # default to -15 = raw deflate  # as there is a separate checksum per chunk supported elsewhere
    # memLevel: int  # default to 8
    # strategy: int  # default to 0 = Z_DEFAULT_STRATEGY

class Gzip(KnownCompressionAlgorithm):
    type: Literal[CompressionAlgorithm.GZIP] = CompressionAlgorithm.GZIP
    compressionParameters: GzipCompressionParameters



class LZ4Mode(Enum):
    FRAME = 0    # streaming-safe framed format (default)
    BLOCK = 1    # raw block, no frame overhead

class LZ4CompressionParameters(BaseModel):
    mode: LZ4Mode = Field(
        default=LZ4Mode.FRAME,
        description='FRAME (framed, streamable) or BLOCK (raw, lower overhead)',
    )
    compressionLevel: int = Field(
        default=0,
        ge=0, le=16,
        description='Frame mode: 0 = fast mode; 1–16 = HC (high-compression) levels. Ignored in BLOCK mode',
    )
    blockSize: int = Field(
        default=0,
        description='Frame mode block size (bytes). 0 = library default (~4 MB). Valid non-zero values: 65536, 262144, 1048576, 4194304',
    )
    contentChecksum: bool = Field(
        default=True,
        description='Frame mode: append xxHash-32 checksum for integrity checks',
    )
    acceleration: int = Field(
        default=1,
        ge=1,
        description='Block mode acceleration factor (≥1). Higher = faster but larger. Ignored in FRAME mode',
    )
    storeSize: bool = Field(
        default=True,
        description='Block mode: prepend original size to the compressed bytes',
    )

    @model_validator(mode="after")
    def _check_block_size(self) -> LZ4CompressionParameters:
        valid = {0, 65536, 262144, 1048576, 4194304}
        if self.blockSize not in valid:
            raise ValueError(f"block_size must be one of {valid}")
        return self

class Lz4(KnownCompressionAlgorithm):
    type: Literal[CompressionAlgorithm.LZ4] = CompressionAlgorithm.LZ4
    compressionParameters: LZ4CompressionParameters



class ZstdStrategy(Enum):
    FAST        = 1
    DFAST       = 2
    GREEDY      = 3
    LAZY        = 4
    LAZY2       = 5
    BTLAZY2     = 6
    BTOPT       = 7
    BTULTRA     = 8
    BTULTRA2    = 9


class ZstdCompressionParameters(BaseModel):
    level: int = Field(
        default=3,
        ge=1, le=22,
        description='Compression level (1=fastest ... 22=best ratio). Levels above 19 use more memory. Default to 3',
    )
    threads: int = Field(
        default=0,
        ge=0,
        description='Worker threads for multi-threaded compression. 0 = single-threaded (default)',
    )
    writeChecksum: bool = Field(
        default=True,
        description='Embed xxHash-64 checksum in the frame for integrity verification',
    )
    writeContentSize: bool = Field(
        default=True,
        description='Store the original size in the frame header',
    )
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

class Zstd(KnownCompressionAlgorithm):
    type: Literal[CompressionAlgorithm.ZSTD] = CompressionAlgorithm.ZSTD
    compressionParameters: ZstdCompressionParameters



class CompressedData(BaseModel):
    compressionAlgorithm: KnownCompressionAlgorithm
    compressedBytes: bytes


def decompress_data_to_chunks_exn(data: CompressedData):
    match data.compressionAlgorithm.type:
        case CompressionAlgorithm.GZIP:
            yield from gzip_decompress(data.compressionAlgorithm.compressionParameters, data.compressedBytes)
        case CompressionAlgorithm.ZSTD:
            pass
        case CompressionAlgorithm.LZ4:
            pass
        case _:
            raise NotImplementedError

def decompress_data_raw_exn(data: CompressedData):
    return b''.join(decompress_data_to_chunks_exn(data))


class CompressionResult(BaseModel):
    algorithm: KnownCompressionAlgorithm
    originalSize: int
    compressedSize: int
    compressionRatio: float
    compressTime: DefaultBaseType.TIMEDELTA | None
    decompressTime: DefaultBaseType.TIMEDELTA | None
