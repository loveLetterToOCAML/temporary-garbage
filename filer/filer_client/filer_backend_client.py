from filer.filer_backend.backend_factory import FilerBackendFor, KnownFilerBackendParameters
from filer.filer_backend.backend_proxy_constrained import ConstrainedBackendParameters
from filer.base_exceptions import HashNotMatchingContent, FilerSerialException
from baseimplems.datastreams.stream_event import TransitStatus
from basetypes.implementation.dataformat.hashed import Hashed

from anyio import create_task_group, CapacityLimiter, create_memory_object_stream, Event
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from anyio.streams.file import FileWriteStream
from pydantic import BaseModel

from typing import AsyncIterator, Callable
from contextlib import asynccontextmanager
from sortedcontainers import SortedDict, SortedSet
from dataclasses import dataclass
from datetime import datetime
import contextlib


class ContentStateHandler:

    def __init__(self, on_send: Event | None = None, on_success: Event | None = None, on_failure: Event | None = None):
        self._current_state = TransitStatus.QUEUED
        self._on_send = on_send
        self._on_success = on_success
        self._on_failure = on_failure

    @property
    def current_status(self):
        return self._current_state

    def go_next(self, is_error: bool = False):
        if is_error:
            self._current_state = TransitStatus.FAILED
            self._on_failure and self._on_failure.set()
            return
        match self._current_state:
            case TransitStatus.QUEUED:
                self._current_state = TransitStatus.SENDING
                self._on_send and self._on_send.set()
            case TransitStatus.SENDING:
                self._current_state = TransitStatus.DONE
                self._on_success and self._on_success.set()
            case _:
                raise NotImplementedError

@dataclass
class ContentUpload:
    intentEmittedAt: datetime
    size: int
    status: TransitStatus
    onSend: Event
    onSuccess: Event
    onFailure: Event

class QueueStateHandler:

    def __init__(self, total_size: int):
        self._total_size = total_size
        self._current_space_taken = 0
        self._queued = SortedSet()
        self._in_progress = {}
        self._succeeded = {}
        self._failed = {}

    def can_host_size(self, size: int):
        return self._current_space_taken + size < self._total_size

    def _resolve_queued(self):
        for sz, locator in self._queued:
            if self.can_host_size(sz):
                self._in_progress.add((sz, locator))
                self._current_space_taken += sz
            else:
                return  # as sorted, no matter to go further

    def queue_intent(self, locator: Hashed, size: int):
        self._queued.add((size, locator))

    def failure_for(self, locator: Hashed):
        pass

    def success_for(self, locator: Hashed):
        pass


class FilerBackendClientParameters(BaseModel):
    backendParameters: KnownFilerBackendParameters

    maxSizeInMemUpload: int = 0x1000000
    maxSizeTempfileUpload: int = 0x400000000
    ## in case there is not enough space to store temp data and the stream consumption can be replayed,
    ## we play it once for obtaining size + hash, then replay it for upload with obtained information
    ## in fact, this is not a parameter. If provided data is raw data, OK already there; if data is bytes async iterator
    ## then it depends if its size is < maxSizeTempfileUpload; if callable -> bytes async iterator then it's also ok
    ## since we can call it twice: one to get hash, size, compressed data, and once for real upload
    # allowTwoPassStream: bool = True

    chunkSize: int = 0x1000000
    allowSmallerChunks: bool = True

    concurrentParallelWrites: int = 0x40
    concurrentParallelReads: int = 0x100

    ## compression parameters are in fact skipped there, because it's something that require registry knowledge for
    ## ensuring data type. This will be handled by the FilerServerClientParameters
    ## compressDataAlgorithm: CompressionAlgorithmInstance | None = None  # None means data won't be stored compressed anyway
    # compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size, will upload compressed

    # TODO: handle this
    retryPolicy: None


def filer_backend_config_for_remote_constrained(backendParameters: ConstrainedBackendParameters):
    return FilerBackendClientParameters(
        backendParameters=backendParameters,
        chunkSize=backendParameters.constraintParameters.fixedChunkSize or 0x1000000,
        allowSmallerChunks=backendParameters.constraintParameters.fixedChunkSize <= 0,
        concurrentParallelWrites=backendParameters.constraintParameters.concurrentParallelWrites,
        concurrentParallelReads=backendParameters.constraintParameters.concurrentParallelReads,
        compressDataAlgorithm=backendParameters.compressDataAlgorithm,
        compressThreshold=backendParameters.compressThreshold
    )


class UploadHandler:

    def __init__(self, params,
                 data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]],
                 locator: Hashed | None = None, total_size: int | None = None):
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

class FilerBackendClient:

    def __init__(self, params: FilerBackendClientParameters):
        self._client_params = params

    @contextlib.contextmanager
    def upload_data_one_pass(self, locator: Hashed, expected_length: int) -> AsyncIterator[MemoryObjectSendStream[Hashed]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_receiver
        ):
            upload_handler = UploadHandler(chunk_receiver, locator, expected_length)
            tg.start_soon(upload_handler.handle_upload, self._write_limiter)
            yield chunk_sender

    @contextlib.contextmanager
    def upload_data(self) -> AsyncIterator[MemoryObjectSendStream[bytes]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_receiver
        ):
            upload_handler = UploadHandler(chunk_receiver)
            tg.start_soon(upload_handler.handle_upload, self._write_limiter)
            yield chunk_sender

    async def upload_data_one_pass_from(self, locator: Hashed, expected_length: int, data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]]) -> Hashed:
        upload_handler = UploadHandler(data, locator, expected_length)
        self._task_group.start_soon(upload_handler.handle_upload, self._write_limiter)

    async def upload_data_from(self, data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]]) -> Hashed:
        upload_handler = UploadHandler(data)
        self._task_group.start_soon(upload_handler.handle_upload, self._write_limiter)

    @contextlib.contextmanager
    async def download_data(self, locator: Hashed) -> AsyncIterator[MemoryObjectReceiveStream[bytes]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](0x100)
        async with (
            create_task_group() as tg,
            chunk_sender
        ):
            download_handler = DownloadHandler(chunk_sender, locator)
            tg.start_soon(download_handler.handle_download, self._read_limiter)
            yield chunk_receiver

    async def download_data_to(self, locator: Hashed, chunk_sender: MemoryObjectSendStream[bytes]):
        async with chunk_sender:
            download_handler = DownloadHandler(chunk_sender, locator)
            self._task_group.start_soon(download_handler.handle_download, self._read_limiter)

    async def delete_data(self, locator: Hashed):
        return await self._backend.delete_content(locator)

    async def list_available(self) -> AsyncIterator[Hashed]:
        async for locator in self._backend.list_valid_resources():
            yield locator

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._backend = FilerBackendFor(self._client_params.backendParameters)
        self._read_limiter = CapacityLimiter(self._client_params.concurrentParallelReads)
        self._write_limiter = CapacityLimiter(self._client_params.concurrentParallelWrites)

        if not hasattr(self._backend, '__asynccontextmanager__'):
            async with create_task_group() as self._task_group:
                yield self
            return

        async with (
            create_task_group() as self._task_group,
            self._backend
        ):
            yield self
