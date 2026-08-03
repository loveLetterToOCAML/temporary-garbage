from filer.base_exceptions import HashNotMatchingContent, FilerSerialException, OutOfConstraints, FilerConstraintType, \
    ExpectedAgainstReality, PredicateType
from basetypes.implementation.dataformat.hashed import Hashed, SHA256, hash_protocol_for_type, MixedMd5Sha256
from filer.filer_backend.backend_impl_fs_opti import EffectfulFilerFsBackend, FilerBackendOptimizedFsParameters
from filer.filer_backend.backend_impl_inmem import EffectfulFilerInMemBackend, FilerBackendInMemParameters
from filer.filer_backend.backend_factory import FilerBackendFor, KnownFilerBackendParameters
from filer.filer_backend.utils_temp import enclose_within_temporary_dir_interactive_mock
from filer.filer_backend.backend_proxy_constrained import ConstrainedBackendParameters
from filer.filer_backend.backend_protocol import EffectfulFilerBackend
from baseimplems.datastreams.stream_event import TransitStatus
from filer.filer_backend.backend_failure import BackendFailure
from policy.log import run_with_log_policy, LogLevel
from baseimplems.date_utils import utc_now

from anyio import create_task_group, CapacityLimiter, create_memory_object_stream, Event, AsyncContextManagerMixin, \
    sleep, CancelScope
from anyio.streams.memory import MemoryObjectSendStream, MemoryObjectReceiveStream
from pydantic import BaseModel

from typing import AsyncIterator, Callable, Any
from contextlib import asynccontextmanager
from sortedcontainers import SortedSet
from dataclasses import dataclass
from datetime import datetime
import contextlib


@dataclass
class ContentUpload:
    intentEmittedAt: datetime
    size: int
    status: TransitStatus
    onSend: Event

class QueueStateHandler:

    def __init__(self, total_size: int):
        self._total_size = total_size
        self._current_space_taken = 0
        self._queued = SortedSet()
        self._in_progress = set()
        self._succeeded = {}
        self._failed = {}
        self._state_for_locator: dict[int, ContentUpload] = {}

    @property
    def storage_capcacity(self):
        return self._total_size

    def can_host_size(self, size: int):
        return self._current_space_taken + size < self._total_size

    def _resolve_queued(self):
        to_remove = []
        for sz, locator in self._queued:
            if self.can_host_size(sz):
                self._in_progress.add((sz, locator))
                self._current_space_taken += sz
                cu = self._state_for_locator[locator]
                cu.status = TransitStatus.SENDING
                cu.onSend.set()
                to_remove.append((sz, locator))
            else:
                break  # as sorted, no matter to go further

        for sz, locator in to_remove:
            self._queued.remove((sz, locator))

    def queue_intent(self, locator: int, size: int):
        if locator in self._state_for_locator:
            raise Exception('Should never happen, caller must be careful not to call twice with same locator')
        self._queued.add((size, locator))
        send_event = Event()
        cu = ContentUpload(intentEmittedAt=utc_now(), size=size, status=TransitStatus.QUEUED, onSend=send_event)
        self._state_for_locator[locator] = cu
        self._resolve_queued()
        return cu

    def failure_for(self, locator: int):
        cu = self._state_for_locator[locator]
        cu.status = TransitStatus.FAILED
        self._in_progress.remove((cu.size, locator))
        self._current_space_taken -= cu.size
        self._failed[locator] = cu
        self._resolve_queued()

    def success_for(self, locator: int):
        cu = self._state_for_locator[locator]
        cu.status = TransitStatus.DONE
        self._in_progress.remove((cu.size, locator))
        self._current_space_taken -= cu.size
        self._succeeded[locator] = cu
        self._resolve_queued()


class FilerBackendClientParameters(BaseModel):
    backendParameters: KnownFilerBackendParameters

    maxSizeInMemUpload: int = 0x1000000
    maxSizeTempfileUpload: int = 0x400000000
    proportionOfStorageWaitBelow: float = 0.6  # from 0.6 * 0x400000000, we don't even try to wait the delay if no space
    maxDelayWaitTempUpload: float = 600  # if we need 2 passes and no space remain in temporary upload, wait this delay
    ## in case there is not enough space to store temp data and the stream consumption can be replayed,
    ## we play it once for obtaining size + hash, then replay it for upload with obtained information
    ## in fact, this is not a parameter. If provided data is raw data, OK already there; if data is bytes async iterator
    ## then it depends if its size is < maxSizeTempfileUpload; if callable -> bytes async iterator then it's also ok
    ## since we can call it twice: one to get hash, size, compressed data, and once for real upload
    # allowTwoPassStream: bool = True

    chunkSize: int = 0x1000000
    allowSmallerChunks: bool = True
    allowBiggerChunks: bool = True

    concurrentParallelWrites: int = 0x40

    ## compression parameters are in fact skipped there, because it's something that require registry knowledge for
    ## ensuring data type. This will be handled by the FilerServerClientParameters
    ## compressDataAlgorithm: CompressionAlgorithmInstance | None = None  # None means data won't be stored compressed anyway
    # compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size, will upload compressed

    # TODO: handle this
    retryPolicy: None = None


def filer_backend_config_for_remote_constrained(backendParameters: ConstrainedBackendParameters, **additional):
    return FilerBackendClientParameters(
        backendParameters=backendParameters,
        chunkSize=backendParameters.constraintParameters.fixedChunkSize or 0x1000000,
        allowSmallerChunks=backendParameters.constraintParameters.fixedChunkSize <= 0,
        allowBiggerChunks=backendParameters.constraintParameters.fixedChunkSize <= 0,
        concurrentParallelWrites=backendParameters.constraintParameters.concurrentParallelWrites,
        **additional
    )


class UploadHandler:

    def __init__(self, input_data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]],
                 chunk_size: int, backend: EffectfulFilerBackend[Hashed, Any, BackendFailure], *,
                 expected_length: int | None = None, maximum_length: int | None = None, locator: Hashed | None = None,
                 allow_smaller_chunks: bool = True, allow_bigger_chunks: bool = True):
        self._input_data = input_data
        self._chunk_size = chunk_size
        self._backend = backend
        self._expected_length = expected_length
        self._maximum_length = maximum_length
        self._locator: Hashed = locator
        self._allow_smaller_chunks = allow_smaller_chunks
        self._allow_bigger_chunks = allow_bigger_chunks

        if self._locator and isinstance(self._input_data, bytes):
            self._chunk_generator = self.generate_chunks_from_raw_data
        elif self._locator and hasattr(self._input_data, '__call__'):
            self._chunk_generator = self._input_data
        elif self._locator and hasattr(self._input_data, '__aiter__'):
            self._chunk_generator = lambda: self._input_data
        else:
            self._chunk_generator = None

    async def generate_chunks_from_raw_data(self):
        for offset in range(0, self._expected_length, self._chunk_size):
            yield self._input_data[offset: offset + self._chunk_size]

    def _update_locator_and_data_generator_from_raw_data(self):
        self._expected_length = len(self._input_data)
        with hash_protocol_for_type(MixedMd5Sha256()).compute_new() as h:
            for offset in range(0, self._expected_length, self._chunk_size):
                h.update(self._input_data[offset: offset + self._chunk_size])
            self._locator = Hashed(hashAlgorithm=MixedMd5Sha256(), hash=h.digest())
        self._chunk_generator = self.generate_chunks_from_raw_data

    async def _update_locator_and_data_generator_after_first_pass(self):
        self._expected_length = 0
        with hash_protocol_for_type(MixedMd5Sha256()).compute_new() as h:
            async for chunk in self._input_data():
                h.update(chunk)
                self._expected_length += len(chunk)
            self._locator = Hashed(hashAlgorithm=MixedMd5Sha256(), hash=h.digest())
        self._chunk_generator = self._input_data

    async def _update_locator_and_data_generator_after_cache(
            self, placeholder_index: int, chunk_generator: AsyncIterator[bytes],
            cache_backend: EffectfulFilerBackend[Hashed, Any, BackendFailure] | None = None
    ):
        with hash_protocol_for_type(SHA256()).compute_new() as h:
            h.update(bytes([placeholder_index]))
            fake_locator = Hashed(hashAlgorithm=SHA256(), hash=h.digest())
        await cache_backend.prepare_placeholder_for_hash_exn(fake_locator, placeholder_index, self._maximum_length)
        cur_offset = 0
        with hash_protocol_for_type(MixedMd5Sha256()).compute_new() as h:
            async for chunk in chunk_generator:
                await cache_backend.upload_chunk_for_hash_exn(fake_locator, placeholder_index, cur_offset, chunk)
                cur_offset += len(chunk)
                h.update(chunk)
            await cache_backend.upload_terminate_for_hash_exn(fake_locator, placeholder_index)
            self._locator = Hashed(hashAlgorithm=MixedMd5Sha256(), hash=h.digest())
            self._expected_length = cur_offset

            async def generate_data_from_cache():
                for offset in range(0, self._expected_length, self._chunk_size):
                    yield await cache_backend.download_chunk_for_hash_exn(fake_locator, offset, self._chunk_size)
            self._chunk_generator = generate_data_from_cache

    async def handle_upload(self, write_limiter, placeholder_index: int,
                            on_success: Callable | None = None, on_failure: Callable | None = None,
                            cache_backend: EffectfulFilerBackend[Hashed, Any, BackendFailure] | None = None):
        try:
            result = await self._handle_upload_internal(write_limiter, placeholder_index, cache_backend)
            on_success and on_success()
            return result
        except Exception as e:
            on_failure and on_failure()
            raise e

    async def _handle_upload_internal(self, write_limiter, placeholder_index: int,
                                      cache_backend: EffectfulFilerBackend[Hashed, Any, BackendFailure] | None = None):
        async with write_limiter:
            if not self._locator:
                if isinstance(self._input_data, bytes):
                    self._update_locator_and_data_generator_from_raw_data()

            if not self._locator and cache_backend:  # this is required to get locator
                if hasattr(self._input_data, '__aiter__'):
                    await self._update_locator_and_data_generator_after_cache(placeholder_index, self._input_data, cache_backend)
                else:  # case Callable[[], AsyncIterator[bytes]]
                    await self._update_locator_and_data_generator_after_cache(placeholder_index, self._input_data(), cache_backend)

            if not self._locator and hasattr(self._input_data, '__call__'):
                await self._update_locator_and_data_generator_after_first_pass()

            if not self._locator:
                raise Exception('Should not happen: no locator obtained before effectful upload')

            with self._locator.compute_new() as h:
                await self._backend.prepare_placeholder_for_hash_exn(self._locator, placeholder_index, self._expected_length)

                buffer = b''
                cur_offset = 0

                async def send_buffer(max_size: int):
                    nonlocal cur_offset, buffer
                    await self._backend.upload_chunk_for_hash_exn(self._locator, placeholder_index, cur_offset, buffer[:max_size])
                    cur_offset += len(buffer[:max_size])
                    buffer = buffer[max_size:]
                    return

                async for chunk in self._chunk_generator():
                    buffer += chunk
                    if len(buffer) < self._chunk_size and self._allow_smaller_chunks:
                        await send_buffer(len(buffer))
                    elif len(buffer) > self._chunk_size and self._allow_bigger_chunks:
                        await send_buffer(len(buffer))
                    elif len(buffer) >= self._chunk_size:
                        while len(buffer) >= self._chunk_size:
                            await send_buffer(self._chunk_size)

                    if cur_offset > self._expected_length:
                        await self._backend.delete_content_exn(self._locator, placeholder_index)
                        raise FilerSerialException(
                            OutOfConstraints(
                                failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                                details=ExpectedAgainstReality[int](expectationType=PredicateType.INFERIOR,
                                                                    reference=self._expected_length,
                                                                    got=cur_offset)
                            )
                        )
                    h.update(chunk)

                if buffer:  # last potential chunk
                    await send_buffer(len(buffer))

                if not h.is_same(self._locator.hash):
                    await self._backend.delete_content_exn(self._locator, placeholder_index)
                    raise FilerSerialException(
                        HashNotMatchingContent(
                            inputHash=h.digest(),
                            expectedHash=self._locator.hash
                        )
                    )
                if self._expected_length != cur_offset:
                    await self._backend.delete_content_exn(self._locator, placeholder_index)
                    raise FilerSerialException(
                        OutOfConstraints(
                            failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                            details=ExpectedAgainstReality[int](expectationType=PredicateType.EQUALS,
                                                                reference=self._expected_length,
                                                                got=cur_offset)
                        )
                    )

                await self._backend.upload_terminate_for_hash(self._locator, placeholder_index)


class DownloadHandler:

    def __init__(self, locator: Hashed, chunk_size: int, backend: EffectfulFilerBackend[Hashed, Any, BackendFailure],
                 memory_stream: MemoryObjectReceiveStream[bytes] | None = None):
        self._locator = locator
        self._chunk_size = chunk_size
        self._backend = backend
        self._memory_stream = memory_stream

    async def handle_download(self):
        async for chunk in self.handle_download_gen():
            await self._memory_stream.send(chunk)
        await self._memory_stream.aclose()

    async def handle_download_gen(self):
        with self._locator.compute_new() as h:
            size = await self._backend.size_for_hash(self._locator)
            total_size = 0
            for offset in range(0, size, self._chunk_size):
                chunk = await self._backend.download_chunk_for_hash(self._locator, offset, self._chunk_size)
                if not isinstance(chunk, bytes):  # TODO: handle error case more nicely
                    raise chunk.originalException
                total_size += len(chunk)
                if total_size > size:
                    raise FilerSerialException(
                        OutOfConstraints(
                            failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                            details=ExpectedAgainstReality[int](expectationType=PredicateType.INFERIOR,
                                                                reference=size,
                                                                got=total_size)
                        )
                    )
                yield chunk
                h.update(chunk)
            if not h.is_same(self._locator.hash):
                raise FilerSerialException(
                    HashNotMatchingContent(
                        inputHash=h.digest(),
                        expectedHash=self._locator.hash
                    )
                )
            if total_size != size:
                raise FilerSerialException(
                    OutOfConstraints(
                        failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                        details=ExpectedAgainstReality[int](expectationType=PredicateType.EQUALS,
                                                            reference=size,
                                                            got=total_size)
                    )
                )


# TODO: proper exceptions
class RetryLater(Exception):
    pass


class FilerBackendClient(AsyncContextManagerMixin):

    def __init__(self, params: FilerBackendClientParameters):
        self._client_params = params


    def _cache_for_queue(self, queue: QueueStateHandler):
        return self._inmem_cache if queue == self._queue_inmem else self._fs_cache

    def _check_if_available_queue(self, locator: int, size: int):
        for queue in (self._queue_inmem, self._queue_fs):
            if queue.can_host_size(size):
                return queue.queue_intent(locator, size), queue

        # otherwise all queues are full, check if asked size is inferior to threshold
        for queue in (self._queue_inmem, self._queue_fs):
            if size <= queue.storage_capcacity * self._client_params.proportionOfStorageWaitBelow:
                return queue.queue_intent(locator, size), queue

    async def _timeout_after(self, cancel_scope: CancelScope):
        await sleep(self._client_params.maxDelayWaitTempUpload)
        cancel_scope.cancel('maximum time elapsed')

    async def _wait_for_cache_ready(self, locator: int, maximum_size: int):
        queue = None
        async with create_task_group() as tg:
            tg.start_soon(self._timeout_after, tg.cancel_scope)
            queued = self._check_if_available_queue(locator, maximum_size)
            if not queued:
                raise RetryLater(f"Maximum size too big to enter local cache queue (asked {maximum_size} while max {self._queue_fs.storage_capcacity})")
            elem, queue = queued
            await elem.onSend.wait()
            tg.cancel_scope.cancel()
            return self._cache_for_queue(queue), lambda: queue.success_for(locator), lambda: queue.failure_for(locator)
        if queue:
            queue.failure_for(locator)
        raise RetryLater()

    def _optimized_buffer_size(self):  # max 4 Mb buffer size
        return 0x400000 // self._client_params.chunkSize

    @contextlib.asynccontextmanager
    async def upload_data_one_pass(self, locator: Hashed, expected_length: int) -> AsyncIterator[MemoryObjectSendStream[Hashed]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](self._optimized_buffer_size())
        async with (
            chunk_receiver,
            create_task_group() as tg,
        ):
            upload_handler = UploadHandler(chunk_receiver, self._client_params.chunkSize, self._backend,
                                           expected_length=expected_length, locator=locator,
                                           allow_smaller_chunks=self._client_params.allowSmallerChunks,
                                           allow_bigger_chunks=self._client_params.allowBiggerChunks)
            tg.start_soon(upload_handler.handle_upload, self._write_limiter, self._placeholder_index)
            self._placeholder_index += 1
            yield chunk_sender

    @contextlib.asynccontextmanager
    async def upload_data(self, maximum_length: int) -> AsyncIterator[MemoryObjectSendStream[bytes]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](self._optimized_buffer_size())
        async with (
            chunk_receiver,
            create_task_group() as tg,
        ):
            upload_handler = UploadHandler(chunk_receiver, self._client_params.chunkSize, self._backend,
                                           maximum_length=maximum_length,
                                           allow_smaller_chunks=self._client_params.allowSmallerChunks,
                                           allow_bigger_chunks=self._client_params.allowBiggerChunks)
            cache, on_success, on_failure = await self._wait_for_cache_ready(self._placeholder_index, maximum_length)
            tg.start_soon(upload_handler.handle_upload, self._write_limiter, self._placeholder_index, on_success, on_failure, cache)
            self._placeholder_index += 1
            yield chunk_sender

    async def upload_data_one_pass_from(self, locator: Hashed, expected_length: int, data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]]) -> Hashed:
        upload_handler = UploadHandler(data, self._client_params.chunkSize, self._backend,
                                       expected_length=expected_length, locator=locator,
                                       allow_smaller_chunks=self._client_params.allowSmallerChunks,
                                       allow_bigger_chunks=self._client_params.allowBiggerChunks)
        await upload_handler.handle_upload(self._write_limiter, self._placeholder_index)
        self._placeholder_index += 1

    async def upload_data_from(self, data: bytes | AsyncIterator[bytes] | Callable[[], AsyncIterator[bytes]], maximum_length: int) -> Hashed:
        upload_handler = UploadHandler(data, self._client_params.chunkSize, self._backend,
                                       maximum_length=maximum_length,
                                       allow_smaller_chunks=self._client_params.allowSmallerChunks,
                                       allow_bigger_chunks=self._client_params.allowBiggerChunks)
        cache, on_success, on_failure = await self._wait_for_cache_ready(self._placeholder_index, maximum_length)
        await upload_handler.handle_upload(self._write_limiter, self._placeholder_index, on_success, on_failure, cache)
        self._placeholder_index += 1


    @contextlib.asynccontextmanager
    async def download_data(self, locator: Hashed) -> AsyncIterator[MemoryObjectReceiveStream[bytes]]:
        chunk_sender, chunk_receiver = create_memory_object_stream[bytes](self._optimized_buffer_size())
        async with (
            chunk_sender,
            create_task_group() as tg,
        ):
            download_handler = DownloadHandler(locator, self._client_params.chunkSize, self._backend, chunk_sender)
            tg.start_soon(download_handler.handle_download)
            yield chunk_receiver

    @contextlib.asynccontextmanager
    async def download_data_to(self, locator: Hashed, chunk_sender: MemoryObjectSendStream[bytes]):
        async with chunk_sender:
            download_handler = DownloadHandler(locator, self._client_params.chunkSize, self._backend, chunk_sender)
            self._task_group.start_soon(download_handler.handle_download)
            yield self

    async def download_data_gen(self, locator: Hashed):
        download_handler = DownloadHandler(locator, self._client_params.chunkSize, self._backend)
        async for chunk in download_handler.handle_download_gen():
            yield chunk


    async def delete_data_exn(self, locator: Hashed):
        return await self._backend.delete_content_exn(locator)

    async def delete_data(self, locator: Hashed):
        return await self._backend.delete_content(locator)

    async def list_available(self) -> AsyncIterator[Hashed]:
        async for locator in self._backend.list_valid_resources_exn():
            yield locator

    @asynccontextmanager
    async def enter_caches(self):
        async with (
            create_task_group() as self._task_group,
            run_with_log_policy(logLevel=LogLevel.INFO),
            enclose_within_temporary_dir_interactive_mock() as main_dir,
            EffectfulFilerFsBackend(FilerBackendOptimizedFsParameters(basePath=main_dir)) as self._fs_cache
        ):
            yield

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._backend = FilerBackendFor(self._client_params.backendParameters)
        self._write_limiter = CapacityLimiter(self._client_params.concurrentParallelWrites)
        self._placeholder_index = 0
        self._inmem_cache = EffectfulFilerInMemBackend(
            params=FilerBackendInMemParameters(allowedMemory=self._client_params.maxSizeInMemUpload)
        )
        self._queue_inmem = QueueStateHandler(self._client_params.maxSizeInMemUpload)
        self._queue_fs = QueueStateHandler(self._client_params.maxSizeTempfileUpload)

        if not hasattr(self._backend, '__asynccontextmanager__'):
            async with self.enter_caches():
                yield self
        else:
            async with (
                self._backend,
                self.enter_caches()
            ):
                yield self


if __name__ == '__main__':
    from filer.filer_backend.backend_impl_s3 import FilerBackendS3Parameters

    import anyio

    @asynccontextmanager
    async def define_and_run_backend_and_client():
        ENDPOINT_URL = "http://localhost:9000"
        ACCESS_KEY = "myappkey"
        SECRET_KEY = "myappsecret123"
        BUCKET = "my-bucket"

        backend_params = FilerBackendS3Parameters(
            url=ENDPOINT_URL,
            accessKey=ACCESS_KEY,
            secretKey=SECRET_KEY,
            bucketName=BUCKET
        )
        client_params = FilerBackendClientParameters(
            backendParameters=backend_params,
            chunkSize=backend_params.fixedChunkSize,
            allowSmallerChunks=False,
            allowBiggerChunks=False,
        )
        fbc = FilerBackendClient(params=client_params)
        async with fbc:
            yield fbc

    async def main():
        async with define_and_run_backend_and_client() as client:
            async for hash in client.list_available():
                print(hash)

                # data to download is hashAlgorithm=HashAlgorithmInstance(type=<HashAlgorithm.MIXED_MD5_SHA256: 3>, hashParameters=None) hash=b"\x10m`\xa7P\x8f\xc5\x81\x9dI\x8d;\xdc\xaa\xc8{\xf8\x0eU\xac3\x13\x0f(\x9ai}\xedF'\x0b\xd6Y\nY\x8d\x03\xa6=\rFH\x04\xfb\xa8Um\xe8"
                # to relate to backend_impl_s3.py main with double C:\\windows\\system32\\wmp.dll

                data = b''
                print("doing dl 1")
                async for chunk in client.download_data_gen(hash):
                    print(hex(len(chunk)))
                    data += chunk[:-1]

                print("doing dl 2")
                send, recv = create_memory_object_stream[bytes]()
                async with client.download_data_to(hash, send):
                    async for chunk in recv:
                        print(hex(len(chunk)))

                print("doing dl 3")
                async with client.download_data(hash) as recv:
                    async for chunk in recv:
                        print(hex(len(chunk)))

                h = Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'Vl.\xa8~+Vt\xc8\x8f\xe8\x93\xc73\xda\x1cm:\xa9\x12\xe8\xffpH\x08(\xe8\nzb\xa2 \xbe\xc4\x92=\xdf^|\xca\x8f\xef\xa3U\x0f\n\x9b\x85')

                async def async_data_gen():
                    for offset in range(0, len(data), 0x100000):
                        yield data[offset: offset + 0x100000]

                def async_data_send():
                    return async_data_gen()


                print("deletion before dl 1", await client.delete_data(h))
                print("doing up 1")
                async with (
                    client.upload_data(0x1000000) as send_obj,
                    send_obj
                ):
                    with hash_protocol_for_type(MixedMd5Sha256()).compute_new() as h:
                        for offset in range(0, len(data), 0x100000):
                            await send_obj.send(data[offset: offset+0x100000])
                            h.update(data[offset: offset+0x100000])
                    h = Hashed(hashAlgorithm=MixedMd5Sha256(), hash=h.digest())

                print("data deletion returned", await client.delete_data(h))
                print("doing up 2")
                async with (
                    client.upload_data_one_pass(h, len(data)) as send_obj,
                    send_obj
                ):
                    for offset in range(0, len(data), 0x100000):
                        await send_obj.send(data[offset: offset + 0x100000])

                data_or_generator = [
                    data,
                    async_data_gen,
                    async_data_send
                ]

                for x in data_or_generator:
                    print("data deletion returned", await client.delete_data(h))
                    print("doing up 3")
                    await client.upload_data_from(x, 0x1000000)

                    print("data deletion returned", await client.delete_data(h))
                    print("doing up 4")
                    await client.upload_data_one_pass_from(h, len(data), x)

                return

    anyio.run(main)
