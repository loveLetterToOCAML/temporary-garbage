from filer.filer_backend.backend_impl_inmem import check_final_content_hash_async_exn
from baseimplems.anyio_utils import NotInAsyncContextManager, run_within
from filer.filer_backend.backend_protocol import EffectfulFilerBackend
from filer.filer_backend.backend_failure import BackendFailure
from basetypes.implementation.dataformat.hashed import Hashed
from baseimplems.contextvar_utils import ContextVarWrapper

from anyio import AsyncContextManagerMixin

from typing import Any, TypeVar, AsyncIterator, Protocol, final
from contextlib import asynccontextmanager


BackendFailureType = TypeVar('BackendFailureType')
HashType = TypeVar('HashType')


async def download_stream_for(backend: EffectfulFilerBackend[HashType, Any, BackendFailure], locator: HashType, chunk_size=0x4000000) -> AsyncIterator[bytes]:
    sz = await backend.size_for_hash_exn(locator)
    for offset in range(0, sz, chunk_size):
        yield await backend.download_chunk_for_hash_exn(locator, offset, chunk_size)


class EnsureContentIntegrity(Protocol[HashType]):

    def _download_backend(self) -> EffectfulFilerBackend[Hashed, Any, BackendFailure]:
        ...

    @final
    async def is_hash_valid_for_exn(self, hash: Hashed):
        if hash in cache_of_validated_hashes.get():
            return True
        content_iterator = download_stream_for(self._download_backend(), hash)
        await check_final_content_hash_async_exn(hash, content_iterator)
        cache_of_validated_hashes.add_valid(hash)

    @final
    async def is_hash_valid_for(self, hash: Hashed):
        try:
            await self.is_hash_valid_for_exn(hash)
        except:
            pass


class CacheOfValidHashes(AsyncContextManagerMixin):

    def __init__(self):
        self._cache = None

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._cache = set()
        yield

    def __contains__(self, item: Hashed): # real signature unknown
        return item in self._cache

    def add_valid(self, hash: Hashed):
        print("adding", hash)
        if self._cache is None:
            raise NotInAsyncContextManager('add_valid', 'CacheOfValidHashes')
        self._cache.add(hash)

    def remove_from_cache(self, hash: Hashed):  # filer servers must call this on resource deletion
        if self._cache is None:
            raise NotInAsyncContextManager('add_valid', 'CacheOfValidHashes')
        if hash in self._cache:
            self._cache.remove(hash)


cache_of_validated_hashes = ContextVarWrapper[CacheOfValidHashes]('validated_hashes')
run_with_valid_content_cache = run_within(CacheOfValidHashes, cache_of_validated_hashes, reenter_context=True)


if __name__ == '__main__':
    from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, SHA256, hash_protocol_for_type
    from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters
    from filer.filer_backend.backend_proxy_fsupload import EffectfulFsUploadCache
    from policy.log import run_with_log_policy, LogLevel

    import anyio

    data = b'x' * 0xf8 + b'y' * 0xf08
    chosenHashAlg = MixedMd5Sha256()
    with hash_protocol_for_type(chosenHashAlg).compute_new() as h:
        h.update(data)
        hash = h.to_hashed()

    async def main():
        fs_upload_cache = EffectfulFsUploadCache(FilerBackendInMemParameters())
        async with (
            run_with_valid_content_cache() as cache,
            run_with_log_policy(logLevel=LogLevel.INFO) as _,
            fs_upload_cache
        ):
            cache.add_valid(Hashed(hash=b'aaa', hashAlgorithm=MixedMd5Sha256()))
            print(Hashed(hash=b'aaa', hashAlgorithm=MixedMd5Sha256()) in cache)
            print(Hashed(hash=b'aaab', hashAlgorithm=MixedMd5Sha256()) in cache)
            print(Hashed(hash=b'aaa', hashAlgorithm=SHA256()) in cache)
            cache.remove_from_cache(Hashed(hash=b'aaba', hashAlgorithm=MixedMd5Sha256()))
            print(Hashed(hash=b'aaa', hashAlgorithm=MixedMd5Sha256()) in cache)

            await fs_upload_cache.prepare_placeholder_for_hash_exn(hash, 1, len(data))
            for i in range(0, 0x1000, 0x100):
                await fs_upload_cache.upload_chunk_for_hash_exn(hash, 1, i, data[i:i+0x100])
            await fs_upload_cache.upload_terminate_for_hash_exn(hash, 1)
            await anyio.sleep(100)

    anyio.run(main)
