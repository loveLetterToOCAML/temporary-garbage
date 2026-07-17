from contextlib import asynccontextmanager

from ulid import ULID

from filer.base_exceptions import NotExistingContent, NotEnoughSpaceRemaining, FilerSerialException, \
    AlreadyUploadingContent, NotExistingPlaceholderForUpload, AlreadyUploadedContent, HashNotMatchingContent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailureType, ExternalFailure
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.interval_union_bytes import BytesIntervalUnion
from basetypes.implementation.dataformat.hashed import Hashed, HashContextProtocol
from filer.filer_backend.utils_exn import SerialException

from pydantic import BaseModel

from typing import AsyncIterator, Callable, Iterator, TypeVar

from filer.filer_common.registry_protocol import RegistryInContext



ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')
BackendFailureType = TypeVar('BackendFailureType')
UlidType = TypeVar('UlidType')
HashType = TypeVar('HashType', bound=HashContextProtocol)


class EffectfulFilerWithCacheBackend(EffectfulBackend[HashType, BackendFailure], RegistryInContext[HashType, UlidType, MetadataType]):

    def __init__(self, backend_params: FilerBackendParameters, registry_params):
        self._backend_params = backend_params
        self._registry_params = registry_params
        self._registry = RegistryFactory(registry_params)
        super().__init__(self._registry, self._ensure_registry_initialized)


    @asynccontextmanager
    async def _ensure_registry_initialized(self):
        async with (
            ConstrainedBackendFor(self._backend_params)
            self._registry
        ):
            yield

    async def hash_for_ulid_exn(self, ulid: UlidType) -> HashType | None:
        return await self._registry.hash_for_ulid_exn(ulid)

    async def ulid_for_hash_exn(self, hash: HashType) -> UlidType | None:
        return await self._registry.ulid_for_hash_exn(hash)

    async def check_hash_and_ulid_exn(self, hash: HashType, ulid: UlidType) -> bool | None:
        return await self._registry.check_hash_and_ulid_exn(hash, ulid)

    async def size_for_hash_exn(self, hash: HashType) -> int | None:
        return await self._registry.size_for_hash_exn(hash)

    async def metadata_for_hash_exn(self, hash: HashType) -> MetadataType | bool | None:
        return await self._registry.metadata_for_hash_exn(hash)

    async def old_metadata_for_hash_exn(self, hash: HashType) -> MetadataType | None:
        return await self._registry.old_metadata_for_hash_exn(hash)

    async def new_item_exn(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0) -> UlidType:
        return await self._registry.new_item_exn(hash, item_metadata, size_of_data)

    async def delete_item_exn(self, hash: HashType) -> bool | None:
        return await self._registry.delete_item_exn(hash)

    def exception_to_serialized_failure(self, exn: Exception) -> RegistryFailure:
        return self.exception_to_serialized_failure(exn)


    async def size_of_content_at_exn(self, locator: HashType) -> int:
        sz = await self.size_for_hash_exn(locator)
        if sz:
            return sz
        if self._params.allowedExternalModifications:  # in this case perform a dynamic recheck
            sz = await self._backend.size_of_content_at_exn(locator)
            await self.new_item_exn(locator, new_metadata, sz)
        if not sz:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash
                )
            )
        return sz

    async def _ensure_not_existing(self, locator: Hashed):
        existing_ulid = await self.ulid_for_hash_exn(locator)
        if existing_ulid:
            raise FilerSerialException(
                AlreadyUploadedContent(
                    existingUlid=existing_ulid,
                    hashAttempted=locator.hash
                )
            )

    async def _ensure_existing(self, locator: Hashed):
        existing_md = await self.metadata_for_hash_exn(locator)
        if not existing_md or existing_md is True:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash,
                    hasExisted=existing_md is True
                )
            )

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        await self._ensure_not_existing(locator)
        await self._backend.prepare_placeholder_at_exn(locator, placeholder_index, total_size)

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        await self._ensure_not_existing(locator)
        await self._backend.upload_chunk_at_exn(locator, placeholder_index, offset, data)

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        await self._ensure_not_existing(locator)
        await self._backend.upload_terminate_at_exn(locator, placeholder_index)
        # TODO: check the terminate is ok (hash & size match)
        upload_ok = True
        if upload_ok:
            sz = await self._backend.size_of_content_at_exn(locator)
            await self.new_item_exn(locator, new_metadata, sz)

    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        await self._ensure_existing(locator)
        return await self._backend.download_chunk_from_exn(locator, offset, size)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        if placeholder_index >= 0:
            await self._backend.delete_resource_at_exn(locator, placeholder_index)
        else:
            await self._ensure_existing(locator)
            await self._backend.delete_resource_at_exn(locator, -1)
            await self._registry.delete_item_exn(locator)

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        for hash in self._backend._list_resources_reorganize_exn():
            yield hash

    def exception_to_serialized_failure(self, exn: Exception) -> BackendFailure:
        return self._backend.exception_to_serialized_failure(exn)


if __name__ == '__main__':
    from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, hash_protocol_for_type

    import anyio

    data = b'x' * 0x1000
    chosenHashAlg = MixedMd5Sha256()
    with hash_protocol_for_type(chosenHashAlg).compute_new() as h:
        h.update(data)
        hash = h.to_hashed()

    async def main():
        ebim = EffectfulFilerInMemBackend(params=FilerBackendInMemParameters(allowedMemory=0x10000, maxIntervalParts=0x10))
        try:
            await ebim.prepare_placeholder_at_exn(hash, 0,0x10001)
        except FilerSerialException as e:
            print(e)

        placeholder_idx = 0
        await ebim.prepare_placeholder_at_exn(hash, placeholder_idx, len(data))
        for i in range(0, 0x1000, 0x10):
            await ebim.upload_chunk_at_exn(hash, placeholder_idx, i, data[i: i + 0x10])

        try:
            await ebim.upload_chunk_at_exn(hash, placeholder_idx, i, data[i:i + 0x10])
        except FilerSerialException as e:
            print(e)

        await ebim.upload_terminate_at_exn(hash, placeholder_idx)

        try:
            await ebim.prepare_placeholder_at_exn(hash, placeholder_idx, len(data))
        except FilerSerialException as e:
            print(e)

        async for r in ebim.list_resources_reorganize_exn():
            print(r)

        downloaded = await ebim.download_chunk_from_exn(hash, 0, 0x10000)
        print(downloaded[:0x40], '[...]', len(downloaded))
        await ebim.delete_resource_at_exn(hash)

        try:
            downloaded = await ebim.download_chunk_from_exn(hash, 0, 0x10000)
            print(len(downloaded))
        except FilerSerialException as e:
            print(e)

        downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
        print(downloaded)

    anyio.run(main)
