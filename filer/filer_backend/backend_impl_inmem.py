from filer.base_exceptions import NotExistingContent, NotEnoughSpaceRemaining, FilerSerialException, \
    AlreadyUploadingContent, NotExistingPlaceholderForUpload, AlreadyUploadedContent, HashNotMatchingContent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailureType, ExternalFailure
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.interval_union_bytes import BytesIntervalUnion
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend.utils_exn import SerialException

from pydantic import BaseModel

from typing import AsyncIterator, Callable, Iterator


class FilerBackendInMemParameters(BaseModel):
    allowedMemory: int = 0x40000000
    maxIntervalParts: int = 0x10000


def check_final_content_hash_exn(expected_hash: Hashed, chunk_gen: Callable[[], Iterator[bytes]]):
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
        self._current_size = 0
        self._current_size_max = 0
        self._files_per_hash: dict[Hashed, bytes] = {}
        self._temporaryfiles_per_placeholder_index: dict[int, BytesIntervalUnion] = {}

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
        self._check_existing_content_exn(locator)
        return len(self._files_per_hash[locator])

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        if self._current_size_max + total_size > self._params.allowedMemory:
            raise FilerSerialException(
                NotEnoughSpaceRemaining(
                    requestedSize=total_size,
                    remainingSize=self._params.allowedMemory - self._current_size_max
                )
            )
        # below should be handle by the Safe Backend
        #if locator in self._files_per_hash:
        #    raise FilerSerialException(
        #        AlreadyUploadedContent(
        #            existingUlid=None,
        #            hashAttempted=locator.hash
        #        )
        #    )
        if placeholder_index in self._temporaryfiles_per_placeholder_index:  # should never happen in theory due to auto increment
            raise FilerSerialException(
                AlreadyUploadingContent(
                    hashUploading=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )
        self._temporaryfiles_per_placeholder_index[placeholder_index] = BytesIntervalUnion(total_size)
        self._current_size_max += total_size

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        if placeholder_index not in self._temporaryfiles_per_placeholder_index:
            raise FilerSerialException(
                NotExistingPlaceholderForUpload(
                    inputHash=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )
        written = self._temporaryfiles_per_placeholder_index[placeholder_index].union_from(offset, data)
        if self._temporaryfiles_per_placeholder_index[placeholder_index].number_parts > self._params.maxIntervalParts:
            # todo: remove this from there, as it should be handled in ConstrainedEffectfulBackend
            raise SerialException(
                ExternalFailure(
                    externalFailureType=ExternalFailureType.TriggeredSecurity,
                    humanMessage=f"Too much parts encountered during upload: "
                                 f"{self._temporaryfiles_per_placeholder_index[placeholder_index].number_parts} "
                                 f"instead of max {self._params.maxIntervalParts} expected",
                )
            )
        return written

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        if self._temporaryfiles_per_placeholder_index[placeholder_index].is_complete:
            check_final_content_hash_exn(locator, self._temporaryfiles_per_placeholder_index[placeholder_index].complete_data_gen_exn)
            self._files_per_hash[locator] = self._temporaryfiles_per_placeholder_index[placeholder_index].complete_data_exn()
            self._current_size += len(self._files_per_hash[locator])
        del self._temporaryfiles_per_placeholder_index[placeholder_index]

    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        self._check_existing_content_exn(locator)
        return self._files_per_hash[locator][offset: offset + size]

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        if placeholder_index >= 0:
            self._current_size_max -= self._temporaryfiles_per_placeholder_index[placeholder_index].expected_size
            del self._temporaryfiles_per_placeholder_index[placeholder_index]
            if locator in self._files_per_hash:
                raise Exception(f"{locator} already in files_per_hash even if currently uploading, should not happen")
        else:
            self._current_size -= len(self._files_per_hash[locator])
            del self._files_per_hash[locator]

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
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
