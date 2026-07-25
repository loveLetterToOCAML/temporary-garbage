from filer.filer_backend.content_integrity_cache import EnsureContentIntegrity, download_stream_for
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters, EffectfulFilerFsBackend
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.backend_proxy_constrained import KnownFilerBackendParameters
from filer.filer_backend.backend_factory import FilerBackendFor
from filer.filer_backend.backend_failure import BackendFailure
from basetypes.implementation.dataformat.hashed import Hashed

from anyio import AsyncContextManagerMixin, TemporaryDirectory

from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import AsyncIterator, Any


class EffectfulFsUploadCache(EffectfulBackend[Hashed, BackendFailure],
                             EffectfulFilerBackend[Hashed, Hashed, BackendFailure],
                             EnsureContentIntegrity,
                             AsyncContextManagerMixin):

    def __init__(self, params: KnownFilerBackendParameters):
        self._params = params

    @property
    def _effectful_backend(self) -> EffectfulBackend[Hashed, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: Hashed) -> Hashed | None:
        return locator

    def resource_locator_from_hash(self, hash: Hashed) -> Hashed:
        return hash

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AbstractAsyncContextManager:
        async with TemporaryDirectory(suffix='.fscache') as d:
            self._internal_cache = EffectfulFilerFsBackend(FilerBackendFsParameters(basePath=d))
            self._target_backend = FilerBackendFor(self._params)
            yield self


    async def size_of_content_at_exn(self, locator: Hashed) -> int:
        return await self._target_backend.size_for_hash_exn(locator)

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        return await self._internal_cache.prepare_placeholder_for_hash_exn(locator, placeholder_index, total_size)

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        return await self._internal_cache.upload_chunk_for_hash_exn(locator, placeholder_index, offset, data)

    def _download_backend(self) -> EffectfulFilerBackend[Hashed, Any, BackendFailure]:
        return self._internal_cache

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        try:
            await self._internal_cache.upload_terminate_for_hash_exn(locator, placeholder_index)
            # in this case the hash is wrong, we remove it from the cache and won't upload it to avoid unecessary costs
            # (this will raise)
            await self.is_hash_valid_for_exn(locator)  # this could also be cached from above (in this case this avoids hash recomputation)

            total_size = await self._internal_cache.size_for_hash_exn(locator)
            await self._target_backend.prepare_placeholder_for_hash_exn(locator, placeholder_index, total_size)
            offset = 0
            async for chunk in download_stream_for(self._internal_cache, locator):
                await self._target_backend.upload_chunk_for_hash_exn(locator, placeholder_index, offset, chunk)
                offset += len(chunk)
            await self._target_backend.upload_terminate_for_hash_exn(locator, placeholder_index)
        finally:
            await self._internal_cache.delete_content_exn(locator)

    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        return await self._target_backend.download_chunk_for_hash_exn(locator)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        return await self._target_backend.delete_content_exn(locator)

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        async for rsrc in self._target_backend.list_resources_reorganize_exn():
            yield rsrc

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        return self._target_backend.exception_to_registry_failure(exn)
