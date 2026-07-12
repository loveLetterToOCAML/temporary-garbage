from filer.base_exceptions import NotExistingContent, NotEnoughSpaceRemaining, FilerSerialException, \
    AlreadyUploadingContent, NotExistingPlaceholderForUpload, AlreadyUploadedContent, HashNotMatchingContent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailureType, ExternalFailure
from filer.filer_backend.interval_union_bytes import BytesIntervalUnion
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend.utils_exn import SerialException

from pydantic import BaseModel
from anyio import Lock

from typing import AsyncIterator, Callable, Iterator


class FilerBackendInMemParameters(BaseModel):
    allowedMemory: int = 0x40000000
    maxIntervalParts: int = 0x10000


def check_final_content_hash(expected_hash: Hashed, chunk_gen: Callable[[], Iterator[bytes]]):
    with expected_hash.compute_new() as h:
        for chunk in chunk_gen():
            h.update(chunk)
        if not h.is_same(expected_hash.hash):
            raise FilerSerialException(
                HashNotMatchingContent(
                    inputHash=h.digest(),
                    expectedHash=expected_hash.hash
                )
            )


class EffectfulFilerInMemBackend(EffectfulBackend[Hashed, BackendFailure], EffectfulFilerBackend[Hashed, Hashed, BackendFailure]):
    """
    Simple in mem filer backend with two constraints: not too much fragmentation during upload (maxIntervalParts)
    and maximum allowed memory to store file content (allowedMemory)
    """

    def __init__(self, params: FilerBackendInMemParameters):
        self._params = params
        self._lock = Lock()
        self._current_size = 0
        self._current_size_max = 0
        self._files_per_hash: dict[Hashed, bytes] = {}
        self._temporaryfiles_per_hash: dict[Hashed, BytesIntervalUnion] = {}

    @property
    def _effectful_backend(self) -> EffectfulBackend[Hashed, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: Hashed) -> Hashed | None:
        return locator

    def resource_locator_from_hash(self, hash: Hashed) -> Hashed:
        return hash


    def _check_existing_content_exn(self, locator: Hashed):
        if locator not in self._files_per_hash:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash
                )
            )

    async def size_of_content_at_exn(self, locator: Hashed) -> int:
        async with self._lock:
            self._check_existing_content_exn(locator)
            return len(self._files_per_hash[locator])

    async def prepare_placeholder_at_exn(self, locator: Hashed, total_size: int):
        if self._current_size_max + total_size > self._params.allowedMemory:
            raise FilerSerialException(
                NotEnoughSpaceRemaining(
                    requestedSize=total_size,
                    remainingSize=self._params.allowedMemory - self._current_size_max
                )
            )
        if locator in self._files_per_hash:
            raise FilerSerialException(
                AlreadyUploadedContent(
                    existingUlid=None,
                    hashAttempted=locator.hash
                )
            )
        if locator in self._temporaryfiles_per_hash:
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=locator.hash)
            )
        async with self._lock:
            self._temporaryfiles_per_hash[locator] = BytesIntervalUnion(total_size)
            self._current_size_max += total_size

    async def upload_chunk_at_exn(self, locator: Hashed, offset: int, data: bytes):
        if locator not in self._temporaryfiles_per_hash:
            raise FilerSerialException(
                NotExistingPlaceholderForUpload(
                    inputHash=locator.hash,
                )
            )
        self._temporaryfiles_per_hash[locator].union_from(offset, data)
        if self._temporaryfiles_per_hash[locator].number_parts > self._params.maxIntervalParts:
            raise SerialException(
                ExternalFailure(
                    externalFailureType=ExternalFailureType.TriggeredSecurity,
                    humanMessage=f"Too much parts encountered during upload: {self._temporaryfiles_per_hash[locator].number_parts} instead of max {self._params.maxIntervalParts} expected",
                )
            )

    async def upload_terminate_at_exn(self, locator: Hashed):
        if self._temporaryfiles_per_hash[locator].is_complete:
            check_final_content_hash(locator, self._temporaryfiles_per_hash[locator].complete_data_gen_exn)
            async with self._lock:
                self._files_per_hash[locator] = self._temporaryfiles_per_hash[locator].complete_data_exn()
                self._current_size += len(self._files_per_hash[locator])
        del self._temporaryfiles_per_hash[locator]

    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        async with self._lock:
            self._check_existing_content_exn(locator)
            return self._files_per_hash[locator][offset: offset + size]

    async def delete_resource_at_exn(self, locator: Hashed, placeholder: bool = False):
        if placeholder:
            async with self._lock:
                self._current_size_max -= self._temporaryfiles_per_hash[locator].expected_size
                del self._temporaryfiles_per_hash[locator]
                if locator in self._files_per_hash:
                    raise Exception(f"{locator} already in files_per_hash even if currently uploading, should not happen")
        else:
            async with self._lock:
                self._current_size -= len(self._files_per_hash[locator])
                del self._files_per_hash[locator]

    async def _list_resources_exn(self) -> AsyncIterator[Hashed]:
        for hash in self._files_per_hash:
            yield hash

    def exception_to_serialized_failure(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulFilerInMemBackend exception',
                retryable=False
            )
        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulFilerInMemBackend::InternalError',
            retryable=False,
            originalException=exn
        )


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
            await ebim.prepare_placeholder_at_exn(hash, 0x10001)
        except FilerSerialException as e:
            print(e)

        await ebim.prepare_placeholder_at_exn(hash, len(data))
        for i in range(0, 0x1000, 0x10):
            await ebim.upload_chunk_at_exn(hash, i, data[i: i + 0x10])

        try:
            await ebim.upload_chunk_at_exn(hash, i, data[i:i + 0x10])
        except FilerSerialException as e:
            print(e)

        await ebim.upload_terminate_at_exn(hash)

        try:
            await ebim.prepare_placeholder_at_exn(hash, len(data))
        except FilerSerialException as e:
            print(e)

        async for r in ebim.list_resources_exn():
            print(r)

        downloaded = await ebim.download_chunk_from_exn(hash, 0, 0x10000)
        print(len(downloaded))
        await ebim.delete_resource_at_exn(hash)

        try:
            downloaded = await ebim.download_chunk_from_exn(hash, 0, 0x10000)
            print(len(downloaded))
        except FilerSerialException as e:
            print(e)

        downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
        print(downloaded)

    anyio.run(main)
