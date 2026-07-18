import contextlib
from contextlib import asynccontextmanager

from anyio import AsyncContextManagerMixin, create_task_group, CapacityLimiter, create_memory_object_stream
from anyio.streams.buffered import BufferedByteReceiveStream
from anyio.streams.file import FileWriteStream
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from pydantic import BaseModel

from basetypes.implementation.dataformat.compression import CompressionAlgorithmInstance

from typing import AsyncIterator, TypeVar, Generic

from filer.base_exceptions import HashNotMatchingContent, FilerSerialException

LocatorType = TypeVar('LocatorType')


class FilerBackendClientConfig(BaseModel):
    maxSizeInMemUpload: int = 0x1000000
    maxSizeTempfileUpload: int = 0x400000000
    # in case there is not enough space to store temp data and the stream consumption can be replayed,
    # we play it once for obtaining size + hash, then replay it for upload with obtained information
    allowTwoPassStream: bool = True

    chunkSize: int = 0x1000000
    allowSmallerChunks: bool = True

    concurrentParallelWrites: int = 0x40
    concurrentParallelReads: int = 0x100

    compressDataAlgorithm: CompressionAlgorithmInstance | None = None
    compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size, will store compressed

    retryPolicy: None


class UploadHandler:

    def __init__(self, params, data: bytes | AsyncIterator[bytes], locator: LocatorType | None = None, total_size: int | None = None):
        pass

    def _first_pass_if(self):
        if isinstance(self._data, bytes):
            return len(self._data),

    async def handle_upload(self, write_limiter):
        async with write_limiter:
            cur_size = 0
            await self._backend.prepare_placeholder_for_hash_exn(hash, -1, total_size)
            await self._backend.upload_chunk_for_hash_exn(hash, placeholder_index, total_size)

            async with await FileWriteStream.from_path(path) as stream:
                await stream.send(b'Hello, World!')


class DownloadHandler:

    def __init__(self, params, data: bytes | AsyncIterator[bytes], locator: LocatorType | None = None, total_size: int | None = None):
        pass

    async def handle_download(self, read_limiter):
        async with read_limiter:
            with self._locator.compute_new() as h:
                size = await self._backend.size_for_hash(self._locator)
                for offset in range(0, size, self._chunk_size):
                    chunk = await self._backend.download_chunk_for_hash(hash, offset, self._chunk_size)
                    await self._memory_stream.send(chunk)
                    if isinstance(chunk, bytes):  # TODO: handle error case more nicely
                        raise chunk
                    h.update(chunk)
                if not h.is_same():
                    raise FilerSerialException(
                        HashNotMatchingContent(
                            inputHash=h.digest(),
                            expectedHash=self._locator.hash
                        )
                    )

class FilerBackendClient(Generic[LocatorType]):

    def __init__(self, backend_params: EffectfulFilerBackend):
        self._backend_params = backend_params
        self._read_limiter = None
        self._write_limiter = None

    @contextlib.contextmanager
    def upload_data_one_pass(self, locator: LocatorType, expected_length: int) -> LocatorType:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_receiver
        ):
            upload_handler = UploadHandler(chunk_receiver)
            tg.start_soon(upload_handler.handle_upload, self._write_limiter)
            yield chunk_sender

    @contextlib.contextmanager
    def upload_data(self) -> LocatorType:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_receiver
        ):
            upload_handler = UploadHandler()
            tg.start_soon(upload_handler.handle_upload, self._write_limiter)
            yield chunk_sender

    async def upload_data_one_pass_from(self, locator: LocatorType, expected_length: int, data: bytes | AsyncIterator[bytes]) -> LocatorType:
        upload_handler = UploadHandler(data, locator, expected_length)
        self._task_group.start_soon(upload_handler.handle_upload, self._write_limiter)

    async def upload_data_from(self, data: bytes | AsyncIterator[bytes]) -> LocatorType:
        upload_handler = UploadHandler(data)
        self._task_group.start_soon(upload_handler.handle_upload, self._write_limiter)

    @contextlib.contextmanager
    async def download_data(self, locator: LocatorType):
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_sender
        ):
            download_handler = DownloadHandler(chunk_sender)
            tg.start_soon(download_handler.handle_download, self._read_limiter)
            yield chunk_receiver

    async def download_data_to(self, locator: LocatorType, chunk_sender: MemoryObjectSendStream[bytes]):
        async with chunk_sender:
            download_handler = DownloadHandler(chunk_sender)
            self._task_group.start_soon(download_handler.handle_download, self._read_limiter)

    async def delete_data(self, locator: LocatorType):
        return await self._backend.delete_content(locator)

    async def list_available(self) -> AsyncIterator[LocatorType]:
        async for locator in self._backend.list_resources_reorganize():
            yield locator

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._backend = FilerBackendFactory(self._backend_params)
        self._read_limiter = CapacityLimiter(self._client_params.concurrentParallelReads)
        self._write_limiter = CapacityLimiter(self._client_params.concurrentParallelWrites)

        if self._backend:
            async with create_task_group() as self._task_group:
                yield self
            return

        self._backend = FilerBackendWithContextFactory(self._backend_params)
        async with (
            create_task_group() as self._task_group,
            self._backend
        ):
            yield self


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
    def serialized_exception(self, exn: Exception) -> BackendFailureType:
        return self._effectful_backend.exception_to_registry_failure(exn)

    size_for_hash = final(encapsulate_exception(serialized_exception, size_for_hash_exn))
    prepare_placeholder_for_hash = final(encapsulate_exception(serialized_exception, prepare_placeholder_for_hash_exn))
    upload_chunk_for_hash = final(encapsulate_exception(serialized_exception, upload_chunk_for_hash_exn))
    upload_terminate_for_hash = final(encapsulate_exception(serialized_exception, upload_terminate_for_hash_exn))
    download_chunk_for_hash = final(encapsulate_exception(serialized_exception, download_chunk_for_hash_exn))
    delete_content = final(encapsulate_exception(serialized_exception, delete_content_exn))
    check_integrity_for = final(encapsulate_exception(serialized_exception, check_integrity_for_exn))
    list_resources_reorganize = final(encapsulate_exception(serialized_exception, list_resources_reorganize_exn))
