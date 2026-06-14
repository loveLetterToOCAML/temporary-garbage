from typing import Protocol, final


class CommonDataBufferSyncProcessing(Protocol):

    def __call__(self, data: bytes) -> bytes:
        ...

    def begin(self) -> bytes:
        ...

    def flush(self) -> bytes:
        ...

    def end(self) -> bytes:
        ...


# Usual python compressors are not asynchronous so keep these protocols simple and synchronous

class StreamCompressorProtocol(CommonDataBufferSyncProcessing):

    @final
    def __call__(self, data: bytes) -> bytes:
        return self.compress(data)

    def compress(self, data: bytes) -> bytes:
        ...

    def compress_and_flush(self, data: bytes) -> bytes:
        ...

    @final
    def compress_all(self, data: bytes) -> bytes:
        return self.begin() + self.compress(data) + self.end()


class StreamDecompressorProtocol(CommonDataBufferSyncProcessing):

    @final
    def __call__(self, data: bytes) -> bytes:
        return self.decompress(data)

    def decompress(self, data: bytes) -> bytes:
        ...

    def decompress_and_flush(self, data: bytes) -> bytes:
        ...

    @final
    def decompress_all(self, data: bytes) -> bytes:
        return self.begin() + self.decompress(data) + self.end()


