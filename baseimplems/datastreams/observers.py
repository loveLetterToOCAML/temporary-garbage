from anyio import AsyncContextManagerMixin

from typing import Callable, AsyncIterator
import contextlib


class StatePoller(AsyncContextManagerMixin):

    def __init__(self, external_state: object | Callable):
        self._external_state = external_state

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._resource_died:
            raise StopAsyncIteration
        return self.snapshot()

    @contextlib.asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator:
        self._resource_died = False
        try:
            yield
        finally:
            self._resource_died = True

    def snapshot(self):
        if hasattr(self._external_state, '__call__'):
            return self._external_state()
        else:
            return self._external_state


def run(self, data: bytes):
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