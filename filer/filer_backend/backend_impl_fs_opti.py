from __future__ import annotations

from filer.base_exceptions import FilerSerialException, AlreadyUploadingContent, NotExistingPlaceholder, \
    NotExistingContent, AlreadyUploadedContent
from filer.filer_backend.effectful_fs import FsCreateReserve, fs_side_effect_for, FsUpdateContent, FsMove, \
    FsReadContent, FsDelete, FsList, ExceptionSideEffect
from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, hash_protocol_for_type, Hashed
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters, EffectfulFsBackendSimple
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.utils_exn import SerialException
from policy.log import run_with_log_policy, LogLevel
from log.logging_context import logger_for

from anyio import open_file, AsyncContextManagerMixin, create_task_group, move_on_after, Lock, current_time, sleep, \
    CancelScope, CapacityLimiter

from contextlib import asynccontextmanager
from sortedcontainers import SortedDict
from typing import AsyncIterator, Iterable
from functools import wraps
from pathlib import Path
from os import PathLike
import os


class FilerBackendOptimizedFsParameters(FilerBackendFsParameters):
    delayHoldingHandle: float = 60.0
    maximumSimultaneousHandle: int = 0x200  # keep it this low for linux max which is 1024


class HandleCache(AsyncContextManagerMixin):

    def __init__(self, delay: float, max_handles: int):
        self._delay_keep_after_last_action = delay
        self._maximum_simultaneous_handles = max_handles

    def extend_delay_for(self, locator: str | PathLike[str] | int):
        if locator in self._delay_for:
            prev_delay = self._delay_for[locator]
            if (prev_delay, locator) in self._sorted_per_last:
                del self._sorted_per_last[(prev_delay, locator)]
        self._delay_for[locator] = current_time() + self._delay_keep_after_last_action
        self._sorted_per_last[(self._delay_for[locator], locator)] = locator

    async def open_or_cache(self, locator: str | PathLike[str] | int, mode: str = 'r'):
        if locator in self._cache:
            self.extend_delay_for(locator)
            return self._cache[locator]

        fd = await open_file(locator, mode)
        async with self._global_lock:
            self._cache[locator] = fd
            self._cancel_scopes[locator] = None  # reserve it so that it takes a slot for global length computation
            self.extend_delay_for(locator)

        if len(self._cancel_scopes) >= self._maximum_simultaneous_handles:
            locator = self._sorted_per_last.pop(0)
            scope_to_cancel = self._cancel_scopes[locator]
            if scope_to_cancel:  # very extra precautions in extreme case where it would not have been set below
                scope_to_cancel.cancel('evicting last one from queue')

        self._task_group.start_soon(self.run_with_cancel_scope, locator)
        return self._cache[locator]

    async def dispose(self, locator):
        if locator not in self._cache:
            return
        with CancelScope(shield=True):
            await self._cache[locator].aclose()
            async with self._global_lock:
                self._cancel_scopes[locator].cancel(f"Disposing of cached handle {locator}")
                del self._cancel_scopes[locator]
                # may have been destroyed before if queue was full
                if (self._delay_for[locator], locator) in self._sorted_per_last:
                    del self._sorted_per_last[(self._delay_for[locator], locator)]
                del self._cache[locator]
                del self._delay_for[locator]

    async def run_with_cancel_scope(self, locator):
        async with create_task_group() as tg:
            async with self._global_lock:
                self._cancel_scopes[locator] = tg.cancel_scope
            try:
                await self.launch_filename_watchdog(locator)
            finally:
                await self.dispose(locator)

    async def launch_filename_watchdog(self, locator: str | PathLike[str] | int):
        async with self._capacity_limiter:
            while True:
                cur_time = current_time()
                with move_on_after(self._delay_for[locator] - cur_time) as max_time:
                    await sleep(self._delay_for[locator] - cur_time)

                if not max_time.cancelled_caught:
                    print("nothing received, removing cache for", locator)
                    break

    def terminate_cache(self):
        self._task_group.cancel_scope.cancel('externally cancelled')

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._cache = {}
        self._delay_for = {}
        self._cancel_scopes = {}
        self._sorted_per_last = SortedDict()
        self._global_lock = Lock()
        self._capacity_limiter = CapacityLimiter(self._maximum_simultaneous_handles)
        async with create_task_group() as self._task_group:
            yield self


class EffectfulFsBackendOptimized(EffectfulBackend[Path, BackendFailure], AsyncContextManagerMixin):

    def __init__(self, params: FilerBackendOptimizedFsParameters):
        self._params = params
        self._fs_lgr = logger_for(__name__)

    def _placeholder_path_for(self, path, placeholder_index: int):
        return f"{path}.{placeholder_index}.placeholder"

    async def size_of_content_at_exn(self, locator: Path) -> int:
        f = await self._cache.open_or_cache(locator)
        await f.seek(0, os.SEEK_END)
        return await f.tell()

    async def prepare_placeholder_at_exn(self, locator: Path, placeholder_index: int, total_size: int):
        if os.path.isfile(locator):
            raise FilerSerialException(
                AlreadyUploadedContent(
                    hashAttempted=bytes.fromhex(locator.name)
                )
            )
        placeholder_path = self._placeholder_path_for(locator, placeholder_index)
        if os.path.isfile(placeholder_path):
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=bytes.fromhex(locator.name))
            )
        f = await self._cache.open_or_cache(placeholder_path, 'wb')
        self._fs_lgr.info(f"Placeholder creation {placeholder_path} and reservation of {total_size} bytes",
                          fs_side_effect_for(FsCreateReserve(reservedBytes=total_size), placeholder_path))
        await f.truncate(total_size)

    async def upload_chunk_at_exn(self, locator: Path, placeholder_index: int, offset: int, data: bytes) -> int:
        path = self._placeholder_path_for(locator, placeholder_index)
        f = await self._cache.open_or_cache(path, 'r+b')
        await f.seek(offset)
        await f.write(data)
        self._fs_lgr.info(f"Placeholder write at {path} from {offset} ({len(data)} bytes)",
                          fs_side_effect_for(FsUpdateContent(fromOffset=offset, sizeUpdated=len(data)), path))
        return len(data)

    async def upload_terminate_at_exn(self, locator: Path, placeholder_index: int):
        path = self._placeholder_path_for(locator, placeholder_index)
        await self._cache.dispose(path)
        os.rename(path, locator)
        self._fs_lgr.info(f"Upload finished, placeholder being rewritten from {path} to {locator}",
                          fs_side_effect_for(FsMove(targetPath=f"{locator}"), path))

    async def download_chunk_from_exn(self, locator: Path, offset: int, size: int) -> bytes:
        f = await self._cache.open_or_cache(locator, 'rb')
        await f.seek(offset)
        result = await f.read(size)
        self._fs_lgr.info(f"File read at {locator} from {offset} ({size} bytes max, got {len(result)} bytes)",
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
        f = await self._cache.open_or_cache(path, 'rb')
        chunk = await f.read1(0x1000000)
        while chunk:
            h_instance.update(chunk)
            chunk = await f.read1(0x1000000)
        new_path = EffectfulFilerFsBackend.static_resource_locator_from_hash(
            self._params.basePath, h_instance.to_hashed()
        )
        await self._cache.dispose(path)
        os.rename(path, new_path)
        self._fs_lgr.warning(f"Moving resource from {path} to {new_path}", fs_side_effect_for(FsMove(targetPath=f"{new_path}"), path))
        return new_path

    # TODO: merge this with original BackendFilerFS implem
    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Path]:
        to_rename = []
        for entry in os.scandir(self._params.basePath):
            if os.path.isfile(entry.path):
                path = Path(entry.path)
                h = EffectfulFilerFsBackend.hash_from_resource_locator(path)
                if h:
                    yield path
                    continue
                if self._params.expectsOnlyRightFormatted:
                    continue
                elif self._params.allowRenamingOfBadlyFormatted:
                    to_rename.append(path)
                elif not self._params.allowRenamingOfBadlyFormatted:
                    yield path
        for path in to_rename:
            if checked_path_or_renamed := await self._check_and_reformat(path):
                yield checked_path_or_renamed
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
                    ser_exn = NotExistingPlaceholder(
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

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> Iterable[EffectfulFsBackendOptimized]:
        self._cache = HandleCache(self._params.delayHoldingHandle, self._params.maximumSimultaneousHandle)
        async with self._cache:
            try:
                yield self
            finally:
                self._cache.terminate_cache()


def none_if_exception(f):
    @wraps(f)
    def sub(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except:
            return
    return sub


class EffectfulFilerFsBackend(EffectfulFilerBackend[Hashed, Path, BackendFailure], AsyncContextManagerMixin):

    def __init__(self, params: FilerBackendFsParameters | FilerBackendOptimizedFsParameters):
        self._params = params
        self._must_enter_context = isinstance(params, FilerBackendOptimizedFsParameters)
        self._implem = EffectfulFsBackendOptimized(params) if self._must_enter_context else EffectfulFsBackendSimple(params)

    @classmethod
    @none_if_exception
    def hash_from_resource_locator(self, locator: Path) -> Hashed | None:
        return Hashed.str_deserialize_exn(locator.name)

    @property
    def _effectful_backend(self) -> EffectfulBackend[Path, BackendFailure]:
        return self._implem

    @staticmethod
    def static_resource_locator_from_hash(base_path: Path | str, hash: Hashed) -> Path:
        return Path(base_path) / Hashed.str_serialize(hash)

    def resource_locator_from_hash(self, hash: Hashed) -> Path:
        return self.static_resource_locator_from_hash(self._params.basePath, hash)

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> Iterable[EffectfulFsBackendOptimized | EffectfulFsBackendSimple]:
        if self._must_enter_context:
            async with self._implem:
                yield self
        else:
            yield self


if __name__ == '__main__':
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
            enclose_within_temporary_dir_interactive_mock() as main_dir,
            EffectfulFilerFsBackend(FilerBackendOptimizedFsParameters(
                basePath=main_dir,
                maximumSimultaneousHandle=0x3,
                delayHoldingHandle=2
            )) as ebim
        ):
            print(dyn_lp)
            print(main_dir)
            placeholder_idx = 0
            await ebim.prepare_placeholder_for_hash_exn(hash, placeholder_idx, len(data))
            for i in range(0, 0x1000, 0x10):
                await ebim.upload_chunk_for_hash_exn(hash, placeholder_idx, i, data[i:i+0x10])
            print(main_dir)
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
