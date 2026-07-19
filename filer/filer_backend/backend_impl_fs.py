from filer.base_exceptions import FilerSerialException, AlreadyUploadingContent, NotExistingPlaceholderForUpload, \
    NotExistingContent
from basetypes.implementation.dataformat.hashed import Hashed, HashAlgorithm, HashAlgorithmInstance, \
    check_valid_hash_for_type, MixedMd5Sha256
from filer.filer_backend.effectful_fs import FsCreateReserve, fs_side_effect_for, FsUpdateContent, FsMove, \
    FsReadContent, FsDelete, FsList, ExceptionSideEffect
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.utils_exn import SerialException
from policy.log import run_with_log_policy, LogLevel
from log.logging_context import logger_for

from pydantic import BaseModel
from anyio import open_file

from typing import AsyncIterator, Type
from functools import wraps
from pathlib import Path
import os


def none_if_exception(f):
    @wraps(f)
    def sub(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            return
    return sub


class FilerBackendFsParameters(BaseModel):
    basePath: Path
    expectsOnlyRightFormatted: bool = False
    # if allowing any content with expectsOnlyRightFormatted=False, this will lead to renaming of all the files under web-root with the hash of the file content if True
    allowRenamingOfBadlyFormatted: bool = True
    defaultHashAlgorithm: HashAlgorithmInstance = MixedMd5Sha256()


class EffectfulFilerFsBackend(EffectfulFilerBackend[Hashed, Path, BackendFailure]):

    def __init__(self, params: FilerBackendFsParameters, ChosenImplem: Type | None = None):
        self._params = params
        self._implem = (ChosenImplem or EffectfulFsBackendSimple)(self._params)

    @classmethod
    @none_if_exception
    def hash_from_resource_locator(self, locator: Path) -> Hashed | None:
        fname = locator.name.split('.')[0]
        alg, hash_hex = fname.split('-')
        hash_algorithm_type = HashAlgorithm(int(alg))
        hash_algorithm = HashAlgorithmInstance(type=hash_algorithm_type)
        hash_raw = bytes.fromhex(hash_hex)
        if not check_valid_hash_for_type(hash_algorithm, hash_raw):
            return
        return Hashed(
            hashAlgorithm=hash_algorithm,
            hash=hash_raw
        )

    @property
    def _effectful_backend(self) -> EffectfulBackend[Path, BackendFailure]:
        return self._implem

    @staticmethod
    def static_resource_locator_from_hash(base_path: Path | str, hash: Hashed) -> Path:
        return Path(base_path) / f"{hash.hashAlgorithm.type.value}-{hash.hash.hex()}"

    def resource_locator_from_hash(self, hash: Hashed) -> Path:
        return self.static_resource_locator_from_hash(self._params.basePath, hash)


class EffectfulFsBackendSimple(EffectfulBackend[Path, BackendFailure]):

    def __init__(self, params: FilerBackendFsParameters):
        self._current_placeholder_index = 0
        self._params = params
        self._fs_lgr = logger_for(__name__)  # TODO: shouldn't it be resolved during context management entering?

    def _placeholder_path_for(self, path, placeholder_index: int):
        return f"{path}.{placeholder_index}.placeholder"

    async def size_of_content_at_exn(self, locator: Path) -> int:
        async with await open_file(locator, 'r') as f:
            await f.seek(0, os.SEEK_END)
            return await f.tell()

    async def prepare_placeholder_at_exn(self, locator: Path, placeholder_index: int, total_size: int):
        placeholder_path = self._placeholder_path_for(locator, placeholder_index)
        if os.path.isfile(placeholder_path):
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=bytes.fromhex(locator.name))
            )
        async with await anyio.open_file(placeholder_path, "wb") as f:
            self._fs_lgr.info(f"Placeholder creation {placeholder_path} and reservation of {total_size} bytes",
                              fs_side_effect_for(FsCreateReserve(reservedBytes=total_size), placeholder_path))
            await f.truncate(total_size)

    async def upload_chunk_at_exn(self, locator: Path, placeholder_index: int, offset: int, data: bytes) -> int:
        path = self._placeholder_path_for(locator, placeholder_index)
        async with (
            await anyio.open_file(path, "r+b") as f
        ):
            await f.seek(offset)
            await f.write(data)
            self._fs_lgr.info(f"Placeholder write at {path} from {offset} ({len(data)} bytes)",
                              fs_side_effect_for(FsUpdateContent(fromOffset=offset, sizeUpdated=len(data)), path))
            return len(data)

    async def upload_terminate_at_exn(self, locator: Path, placeholder_index: int):
        path = self._placeholder_path_for(locator, placeholder_index)
        os.rename(path, locator)
        self._fs_lgr.info(f"Upload finished, placeholder being rewritten from {path} to {locator}",
                          fs_side_effect_for(FsMove(targetPath=f"{locator}"), path))

    async def download_chunk_from_exn(self, locator: Path, offset: int, size: int) -> bytes:
        async with (
            await anyio.open_file(locator, "rb") as f
        ):
            await f.seek(offset)
            result = await f.read(size)
            self._fs_lgr.info(f"File read at {locator} from {offset} ({size} bytes)",
                              fs_side_effect_for(FsReadContent(fromOffset=offset, expectedSizeToRead=size, sizeRead=len(result)), locator))
            return result

    async def delete_resource_at_exn(self, locator: Path, placeholder_index: int = -1):
        if placeholder_index < 0:
            os.unlink(locator)
            self._fs_lgr.info(f"Deleting base resource at {locator}", fs_side_effect_for(FsDelete(), locator))
        else:
            path = self._placeholder_path_for(locator, placeholder_index)
            os.unlink(path)
            self._fs_lgr.info(f"Deleting placeholder resource at {path}", fs_side_effect_for(FsDelete(), path))

    async def _check_and_reformat(self, path: Path):
        h_instance = hash_protocol_for_type(self._params.defaultHashAlgorithm).fresh_hash_state()
        async with await anyio.open_file(path, 'rb') as f:
            chunk = await f.read1(0x1000000)
            while chunk:
                h_instance.update(chunk)
                chunk = await f.read1(0x1000000)
        new_path = EffectfulFilerFsBackend.static_resource_locator_from_hash(
            self._params.basePath, h_instance.to_hashed()
        )
        os.rename(path, new_path)
        self._fs_lgr.info(f"Moving resource from {path} to {new_path}", fs_side_effect_for(FsMove(targetPath=f"{new_path}"), path))
        return new_path

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Path]:
        for entry in os.scandir(self._params.basePath):
            if os.path.isfile(entry.path):
                path = Path(entry.path)
                h = EffectfulFilerFsBackend.hash_from_resource_locator(path)
                if h:
                    yield path
                if self._params.expectsOnlyRightFormatted:
                    continue
                elif self._params.allowRenamingOfBadlyFormatted and (checked_path_or_renamed := await self._check_and_reformat(path)):
                    yield checked_path_or_renamed
                elif not self._params.allowRenamingOfBadlyFormatted:
                    yield path
        self._fs_lgr.info(f"Listed resource at {self._params.basePath}", fs_side_effect_for(FsList(), self._params.basePath))

    def _exception_to_serialized_failure(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulFilerFsBackend exception',
                retryable=False
            )

        if isinstance(exn, PermissionError):
            return BackendFailure(
                failure=ExternalFailure(externalFailureType=ExternalFailureType.ForbiddenError),
                humanMessage='FilerException::EffectfulFilerFsBackend::Forbidden',
                retryable=False,
                originalException=exn
            )

        try:
            if isinstance(exn, FileNotFoundError):
                path = Path(exn.filename)
                hash = EffectfulFilerFsBackend.hash_from_resource_locator(path)
                if 'placeholder' in exn.filename:
                    ser_exn = NotExistingPlaceholderForUpload(
                        inputHash=hash.hash,
                        placeholderIndex=int(path.name.split('.')[1])
                    )
                else:
                    ser_exn = NotExistingContent(
                        inputHash=hash.hash,
                    )
                return BackendFailure(
                    failure=ser_exn,
                    humanMessage='FilerException::EffectfulFilerFsBackend::NotFound',
                    retryable=False
                )
        except Exception as exn:  # replace exn with potential exception from above
            pass

        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulFilerFsBackend::InternalError',
            retryable=False,
            originalException=exn
        )

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        processed = self._exception_to_serialized_failure(exn)
        self._fs_lgr.exception(f"Encountered exception {processed}", ExceptionSideEffect(serializedException=processed))
        return processed


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
        async with (
            run_with_log_policy(
                logLevel=LogLevel.INFO,
            ) as dyn_lp,
            enclose_within_temporary_dir_interactive_mock() as main_dir
        ):
            print(dyn_lp)
            ebim = EffectfulFilerFsBackend(FilerBackendFsParameters(basePath=main_dir))

            placeholder_idx = 0
            await ebim.prepare_placeholder_for_hash_exn(hash, placeholder_idx, len(data))
            for i in range(0, 0x1000, 0x10):
                await ebim.upload_chunk_for_hash_exn(hash, placeholder_idx, i, data[i:i+0x10])
            await ebim.upload_terminate_for_hash_exn(hash, placeholder_idx)

            print(await ebim.upload_chunk_for_hash(hash, placeholder_idx, i, data[i:i + 0x10]))
            # await ebim.prepare_placeholder_for_hash(hash, placeholder_idx, len(data))

            async for r in ebim.list_resources_reorganize_exn():
                print(r)

            downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
            print(downloaded)

            print("waiting 15s, please put random things in temp dir, it will relist")
            await anyio.sleep(15)
            async for r in ebim.list_resources_reorganize_exn():
                print(r)

        print(await ebim.delete_content(hash))
        print(await ebim.download_chunk_for_hash(hash, 0, 0x10000))

    anyio.run(main)
