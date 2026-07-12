from basetypes.implementation.dataformat.hashed import HashContextProtocol

from typing import Protocol, final, Callable, TypeVar, AsyncIterator
from functools import wraps


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')
BackendFailureType = TypeVar('BackendFailureType')
HashType = TypeVar('HashType', bound=HashContextProtocol)  # just for the compute_new context manager


def encapsulate_exception(exception_converter: Callable[[BackendFailureType], Exception], f):
    @wraps(f)
    async def encapsulated(self, *args, **kwargs):
        try:
            return await f(self, *args, **kwargs)
        except Exception as e:
            return exception_converter(self, e)
    return encapsulated


class EffectfulBackend(Protocol[ExternalResourceLocatorType, BackendFailureType]):

    async def size_of_content_at_exn(self, locator: ExternalResourceLocatorType) -> int:
        ...

    async def prepare_placeholder_at_exn(self, locator: ExternalResourceLocatorType, total_size: int):
        ...

    async def upload_chunk_at_exn(self, locator: ExternalResourceLocatorType, offset: int, data: bytes):
        ...

    async def upload_terminate_at_exn(self, locator: ExternalResourceLocatorType):
        ...

    async def download_chunk_from_exn(self, locator: ExternalResourceLocatorType, offset: int, size: int) -> bytes:
        ...

    async def delete_resource_at_exn(self, locator: ExternalResourceLocatorType, placeholder: bool = False):
        ...

    async def _list_resources_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
        ...

    def exception_to_serialized_failure(self, exn: Exception) -> BackendFailureType:
        ...


class EffectfulFilerBackend(Protocol[HashType, ExternalResourceLocatorType, BackendFailureType]):

    @property
    def _effectful_backend(self) -> EffectfulBackend[ExternalResourceLocatorType, BackendFailureType]:
        ...

    def hash_from_resource_locator(self, locator: ExternalResourceLocatorType) -> HashType | None:
        ...

    def resource_locator_from_hash(self, hash: HashType) -> ExternalResourceLocatorType:
        ...

    @final
    async def size_for_hash_exn(self, hash: HashType) -> int | None:
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.size_of_content_at_exn(locator)

    @final
    async def prepare_placeholder_for_hash_exn(self, hash: HashType, total_size: int):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.prepare_placeholder_at_exn(locator, total_size)

    @final
    async def upload_chunk_for_hash_exn(self, hash: HashType, offset: int, data: bytes):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.upload_chunk_at_exn(locator, offset, data)

    @final
    async def upload_terminate_for_hash_exn(self, hash: HashType):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.upload_terminate_at_exn(locator)

    @final
    async def download_chunk_for_hash_exn(self, hash: HashType, offset: int, size: int) -> bytes:
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.download_chunk_from_exn(locator, offset, size)

    @final
    async def delete_content_exn(self, hash: HashType, placeholder: bool = False):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.delete_resource_at_exn(locator, placeholder)

    @final
    async def list_resources_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
        async for rsrc in self._effectful_backend._list_resources_exn():
            yield rsrc

    @final
    async def check_integrity_for_exn(self, hash: HashType, *, chunk_size: int = 0x1000000) -> bool:
        with hash.compute_new() as h:
            size = await self.size_for_hash_exn(hash)
            for offset in range(0, size, chunk_size):
                h.update(await self.download_chunk_for_hash_exn(hash, offset, chunk_size))
        return h.is_same()

    @final
    def serialized_exception(self, exn: Exception) -> BackendFailureType:
        return self._effectful_backend.exception_to_serialized_failure(exn)

    size_for_hash = final(encapsulate_exception(serialized_exception, size_for_hash_exn))
    prepare_placeholder_for_hash = final(encapsulate_exception(serialized_exception, prepare_placeholder_for_hash_exn))
    upload_chunk_for_hash = final(encapsulate_exception(serialized_exception, upload_chunk_for_hash_exn))
    upload_terminate_for_hash = final(encapsulate_exception(serialized_exception, upload_terminate_for_hash_exn))
    download_chunk_for_hash = final(encapsulate_exception(serialized_exception, download_chunk_for_hash_exn))
    delete_content = final(encapsulate_exception(serialized_exception, delete_content_exn))
    check_integrity_for = final(encapsulate_exception(serialized_exception, check_integrity_for_exn))
    list_resources = final(encapsulate_exception(serialized_exception, list_resources_exn))
