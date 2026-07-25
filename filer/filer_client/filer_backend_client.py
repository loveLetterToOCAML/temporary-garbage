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
from filer.filer_backend.backend_factory import FilerBackendFor, KnownFilerBackendParameters

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

    compressDataAlgorithm: CompressionAlgorithmInstance | None = None  # None means data won't be stored compressed anyway
    compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size, will upload compressed

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

    def __init__(self, backend_params: KnownFilerBackendParameters):
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
            upload_handler = UploadHandler(chunk_receiver, locator, expected_length)
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
            download_handler = DownloadHandler(chunk_sender, locator)
            tg.start_soon(download_handler.handle_download, self._read_limiter)
            yield chunk_receiver

    async def download_data_to(self, locator: LocatorType, chunk_sender: MemoryObjectSendStream[bytes]):
        async with chunk_sender:
            download_handler = DownloadHandler(chunk_sender, locator)
            self._task_group.start_soon(download_handler.handle_download, self._read_limiter)

    async def delete_data(self, locator: LocatorType):
        return await self._backend.delete_content(locator)

    async def list_available(self) -> AsyncIterator[LocatorType]:
        async for locator in self._backend.list_resources_reorganize():
            yield locator

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._backend = FilerBackendFor(self._backend_params)
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
