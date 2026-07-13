from basetypes.implementation.dataformat.hashed import Hashed
from filer.base_exceptions import FilerSerialException, AlreadyUploadingContent, NotExistingPlaceholderForUpload, \
    NotExistingContent, AlreadyUploadedContent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend

from pydantic import BaseModel
from anyio import open_file

from typing import AsyncIterator, Type
from pathlib import Path
import os

from filer.filer_backend.utils_exn import SerialException


class FilerBackendFsParameters(BaseModel):
    basePath: Path
    expectsOnlyRightFormatted: bool = True
    allowRenamingOfBadlyFormatted: bool = True
    #genericParams: GenericBackendParams


class EffectfulFilerFsBackend(EffectfulFilerBackend[Hashed, str, BackendFailure]):

    def __init__(self, params: FilerBackendFsParameters, ChosenImplem: Type | None = None):
        self._params = params
        self._implem = (ChosenImplem or EffectfulFsBackendSimple)(self._params)

    @property
    def _effectful_backend(self) -> EffectfulBackend[str, BackendFailure]:
        return self._implem

    def hash_from_resource_locator(self, locator: str) -> Hashed | None:
        return Hashed(
            hashAlgorithm=self._params.hashAlgorithm,
            hash=bytes.fromhex(locator.split(os.path.sep)[-1])
        )

    def resource_locator_from_hash(self, hash: Hashed) -> str:
        return os.path.join(self._params.basePath, hash.hash.hex())


class EffectfulFsBackendSimple(EffectfulBackend[str, BackendFailure]):

    def __init__(self, params):
        self._params = params

    def _placeholder_path_for(self, path):
        return f"{path}.placeholder"

    async def size_of_content_at_exn(self, locator: str) -> int:
        async with await open_file(locator, 'r') as f:
            await f.seek(0, os.SEEK_END)
            return await f.tell()

    async def prepare_placeholder_at_exn(self, locator: str, total_size: int):
        if os.path.isfile(locator):
            raise FilerSerialException(
                AlreadyUploadedContent(existingUlid=None, hashAttempted=bytes.fromhex(locator.split(os.path.sep)[-1]))  # arf
            )
        placeholder_path = self._placeholder_path_for(locator)
        if os.path.isfile(placeholder_path):
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=bytes.fromhex(locator.split(os.path.sep)[-1]))  # arf
            )
        async with await anyio.open_file(placeholder_path, "wb") as f:
            await f.truncate(total_size)

    async def upload_chunk_at_exn(self, locator: str, offset: int, data: bytes) -> int:
        async with (
            await anyio.open_file(self._placeholder_path_for(locator), "r+b") as f
        ):
            await f.seek(offset)
            await f.write(data)
            return len(data)

    async def upload_terminate_at_exn(self, locator: str):
        os.rename(self._placeholder_path_for(locator), locator)

    async def download_chunk_from_exn(self, locator: str, offset: int, size: int) -> bytes:
        async with (
            await anyio.open_file(locator, "rb") as f
        ):
            await f.seek(offset)
            return await f.read(size)

    async def delete_resource_at_exn(self, locator: str, placeholder: bool = False):
        if not placeholder:
            os.unlink(locator)
        else:
            os.unlink(self._placeholder_path_for(locator))

    async def _list_resources_exn(self) -> AsyncIterator[str]:
        for entry in os.scandir(self._params.basePath):
            if os.path.isfile(entry.path):
                yield entry.path

    def exception_to_serialized_failure(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulFilerFsBackend exception',
                retryable=False
            )
        if isinstance(exn, FileNotFoundError):
            hash = bytes.fromhex(exn.filename.split(os.path.sep)[-1].split('.placeholder')[0])
            if 'placeholder' in exn.filename:
                ser_exn = NotExistingPlaceholderForUpload(
                    inputHash=hash,
                )
            else:
                ser_exn = NotExistingContent(
                    inputHash=hash,
                )
            return BackendFailure(
                failure=ser_exn,
                humanMessage='FilerException::EffectfulFilerFsBackend::NotFound',
                retryable=False
            )
        if isinstance(exn, PermissionError):
            return BackendFailure(
                failure=ExternalFailure(externalFailureType=ExternalFailureType.ForbiddenError),
                humanMessage='FilerException::EffectfulFilerFsBackend::Forbidden',
                retryable=False,
                originalException=exn
            )
        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulFilerFsBackend::InternalError',
            retryable=False,
            originalException=exn
        )


if __name__ == '__main__':
    from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, hash_protocol_for_type
    from filer.filer_backend.utils_temp import enclose_within_temporary_dir_interactive_mock

    import anyio

    data = b'x' * 0x1000
    chosenHashAlg = MixedMd5Sha256()
    with hash_protocol_for_type(chosenHashAlg).compute_new() as h:
        h.update(data)
        hash = h.to_hashed()

    async def main():
        async with enclose_within_temporary_dir_interactive_mock() as main_dir:
            ebim = EffectfulFilerFsBackend(FilerBackendFsParameters(basePath=main_dir))

            await ebim.prepare_placeholder_for_hash_exn(hash, len(data))
            for i in range(0, 0x1000, 0x10):
                await ebim.upload_chunk_for_hash_exn(hash, i, data[i:i+0x10])
            await ebim.upload_terminate_for_hash_exn(hash)

            print(await ebim.upload_chunk_for_hash(hash, i, data[i:i + 0x10]))
            await ebim.prepare_placeholder_for_hash(hash, len(data))

            async for r in ebim.list_resources_exn():
                print(r)

            downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
            print(len(downloaded))

        print(await ebim.delete_content(hash))
        print(await ebim.download_chunk_for_hash(hash, 0, 0x10000))

    anyio.run(main)
