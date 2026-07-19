from basetypes.implementation.dataformat.hashed import HashContextProtocol

from typing import Protocol, final, Callable, TypeVar, AsyncIterator
from functools import wraps

import traceback
import sys


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')
BackendFailureType = TypeVar('BackendFailureType')
HashType = TypeVar('HashType', bound=HashContextProtocol)  # just for the compute_new context manager


def encapsulate_exception(exception_converter: Callable[[BackendFailureType], Exception], f):
    @wraps(f)
    async def encapsulated(self, *args, **kwargs):
        try:
            return await f(self, *args, **kwargs)
        except Exception as e:
            e.__full_stack__ = traceback.extract_stack(sys._getframe(1)) + traceback.extract_tb(e.__traceback__)
            return exception_converter(self, e)
    return encapsulated


class EffectfulBackend(Protocol[ExternalResourceLocatorType, BackendFailureType]):

    async def size_of_content_at_exn(self, locator: ExternalResourceLocatorType) -> int:
        """query and returns the number of bytes of a content located by locator"""
        ...

    async def prepare_placeholder_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, total_size: int):
        """prepare a placeholder for the potential right content located by locator, for the placeholder unique index given by the caller
            this allows to avoid the same placeholder_index creation method and gives responsibility to the caller"""
        ...

    async def upload_chunk_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, offset: int, data: bytes) -> int:
        """upload piece of content for placeholder with given index, we kept the locator also
           even if we could have kept only the placeholder index
           the function must return the number of new bytes effectively written
           there is no constraint there on the fact sum of every bytes written must give the exact expected total_size"""
        ...

    async def upload_terminate_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int):
        """terminate upload for a given locator and placeholder index, raising for any purpose the deletion failed"""
        ...

    async def download_chunk_from_exn(self, locator: ExternalResourceLocatorType, offset: int, size: int) -> bytes:
        """download arbitrary chunks for a given locator, at any offset and for any size (download constraints will be enforced by a specific backend)"""
        ...

    async def delete_resource_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int = -1):
        """delete arbitrary content at locator, with optional placeholder_index if wanting to delete related placeholders (or -1 for real content)"""
        ...

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
        """internal resource listing function that aims at reconstructing a registry from nothing in case the registry state is lost
            this also has a reorganizing ability (essentially renaming when allowed) to handle bad resources"""
        ...

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailureType:
        """functions converting non-linear exception to backend failure type and give more of a real API facade (see EffectfulFilerBackend)"""
        ...


class EffectfulFilerBackend(Protocol[HashType, ExternalResourceLocatorType, BackendFailureType]):
    """Encapsulation of some EffectfulBackend with final auto safe (no exception) functions, only locator conversion must be implemented"""

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
    async def prepare_placeholder_for_hash_exn(self, hash: HashType, placeholder_index: int, total_size: int):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.prepare_placeholder_at_exn(locator, placeholder_index, total_size)

    @final
    async def upload_chunk_for_hash_exn(self, hash: HashType, placeholder_index: int, offset: int, data: bytes) -> int:
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.upload_chunk_at_exn(locator, placeholder_index, offset, data)

    @final
    async def upload_terminate_for_hash_exn(self, hash: HashType, placeholder_index: int):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.upload_terminate_at_exn(locator, placeholder_index)

    @final
    async def download_chunk_for_hash_exn(self, hash: HashType, offset: int, size: int) -> bytes:
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.download_chunk_from_exn(locator, offset, size)

    @final
    async def delete_content_exn(self, hash: HashType, placeholder_index: int = -1):
        locator = self.resource_locator_from_hash(hash)
        return await self._effectful_backend.delete_resource_at_exn(locator, placeholder_index)

    @final
    async def list_resources_reorganize_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
        async for rsrc in self._effectful_backend._list_resources_reorganize_exn():
            yield rsrc

    @final
    async def check_integrity_for_exn(self, hash: HashType, *, chunk_size: int = 0x1000000) -> bool:
        with hash.compute_new() as h:
            size = await self.size_for_hash_exn(hash)
            for offset in range(0, size, chunk_size):
                h.update(await self.download_chunk_for_hash_exn(hash, offset, chunk_size))
            return h.is_same()

    @final
    def exception_to_backend_failure(self, exn: Exception) -> BackendFailureType:
        return self._effectful_backend.serialize_backend_failure_exception(exn)

    size_for_hash = final(encapsulate_exception(exception_to_backend_failure, size_for_hash_exn))
    prepare_placeholder_for_hash = final(encapsulate_exception(exception_to_backend_failure, prepare_placeholder_for_hash_exn))
    upload_chunk_for_hash = final(encapsulate_exception(exception_to_backend_failure, upload_chunk_for_hash_exn))
    upload_terminate_for_hash = final(encapsulate_exception(exception_to_backend_failure, upload_terminate_for_hash_exn))
    download_chunk_for_hash = final(encapsulate_exception(exception_to_backend_failure, download_chunk_for_hash_exn))
    delete_content = final(encapsulate_exception(exception_to_backend_failure, delete_content_exn))
    check_integrity_for = final(encapsulate_exception(exception_to_backend_failure, check_integrity_for_exn))
    list_resources_reorganize = final(encapsulate_exception(exception_to_backend_failure, list_resources_reorganize_exn))
