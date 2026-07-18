from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters, EffectfulFilerFsBackend
from filer.filer_backend.backend_impl_inmem import check_final_content_hash_async_exn
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.backend_failure import BackendFailure
from basetypes.implementation.dataformat.hashed import Hashed

from anyio import AsyncContextManagerMixin, TemporaryDirectory

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncIterator


async def download_stream_for(backend: EffectfulFilerBackend[Hashed, Any, BackendFailure], locator: Hashed, chunk_size=0x4000000) -> AsyncIterator[bytes]:
    sz = await backend.size_for_hash_exn(locator)
    for offset in range(0, sz, chunk_size):
        yield await backend.download_chunk_for_hash_exn(locator, offset, chunk_size)


class EffectfulFsUploadCache(EffectfulBackend[Hashed, BackendFailure],
                             EffectfulFilerBackend[Hashed, Hashed, BackendFailure],
                             AsyncContextManagerMixin):

    def __init__(self, params):
        self._params = params

    @property
    def _effectful_backend(self) -> EffectfulBackend[Hashed, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: Hashed) -> Hashed | None:
        return locator

    def resource_locator_from_hash(self, hash: Hashed) -> Hashed:
        return hash

    @asynccontextmanager
    def __asynccontextmanager__(self) -> AbstractAsyncContextManager:
        async with TemporaryDirectory(suffix='.fscache') as d:
            self._internal_cache = EffectfulFilerFsBackend(
                FilerBackendFsParameters(basePath=d)
            )
            self._internal = FilerBackendFactory(self._params)
            yield self


    async def size_of_content_at_exn(self, locator: Hashed) -> int:
        return await self._internal.size_for_hash_exn(locator)

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        return await self._internal_cache.prepare_placeholder_for_hash_exn(locator, placeholder_index, total_size)

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        return await self._internal_cache.upload_chunk_for_hash_exn(locator, placeholder_index, offset, data)

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        try:
            await self._internal_cache.upload_terminate_for_hash_exn(locator, placeholder_index)
            content_iterator = download_stream_for(self._internal_cache, locator)
            # in this case the hash is wrong, we remove it from the cache and won't upload it to avoid unecessary costs
            # (this will raise)
            await check_final_content_hash_async_exn(locator, content_iterator)

            total_size = await self._internal_cache.size_for_hash_exn(locator)
            await self._internal.prepare_placeholder_for_hash_exn(locator, placeholder_index, total_size)
            offset = 0
            async for chunk in download_stream_for(self._internal_cache, locator):
                await self._internal.upload_chunk_for_hash_exn(locator, placeholder_index, offset, chunk)
                offset += len(chunk)
            await self._internal.upload_terminate_for_hash_exn(locator)
        finally:
            await self._internal_cache.delete_content_exn(locator)


    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        return await self._internal.download_chunk_for_hash_exn(locator)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        return await self._internal.delete_content_exn(locator)

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        async for rsrc in self._internal.list_resources_reorganize_exn():
            yield rsrc

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        return self._internal.exception_to_registry_failure(exn)
