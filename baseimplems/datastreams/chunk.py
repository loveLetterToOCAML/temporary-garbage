# Default pure python async implem of chunks send / recv strongly constrained by time
from basetypes.implementation.dataformat.chunk import ContentTransferParameters, ContentTransferState, \
    TimedGlobalChunksConstraint, DataChunk
from basetypes.implementation.dataformat.compression_protocols import CommonDataBufferSyncProcessing

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore, move_on_after, \
    EndOfStream, to_thread, Event
from anyio.streams.memory import MemoryObjectReceiveStream
from anyio.abc import ObjectReceiveStream
from pydantic import BaseModel

from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable
from contextvars import ContextVar


# these are to move upper, at interaction layer so that these are globally defined after interaction between 2 parts
content_transfer_timed_constraints = ContextVar[TimedGlobalChunksConstraint]('global_chunk_constraints')
content_transfer_parameters = ContextVar[ContentTransferParameters]('transfer_parameters')



# 16 Mb memory usage by default
class ChunkInMemoryConstraint(BaseModel):
    chunkSize: int = 0x1000000
    numberOfChunks: int = 0x100
    timeBeforeFlush: float = 0.03


default_local_chunk_constraint = ChunkInMemoryConstraint()
default_remote_chunk_constraint = ChunkInMemoryConstraint(
    chunkSize = 0x10000,
    numberOfChunks = 0x1000,
    timeBeforeFlush = 0.1
)


class ChunkedBytes(AsyncContextManagerMixin):
    """ Simple memory caching to craft chunks from raw bytes
    """

    # we choose ObjectReceiveStream and not simpler one because of the context management
    # also one should ensure the upper_stream cannot send more than too much data
    def __init__(self, upper_stream: ObjectReceiveStream[bytes],
                 memory_constraints: ChunkInMemoryConstraint = default_local_chunk_constraint):
        self._upper_stream = upper_stream
        self._memory_constraints = memory_constraints

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[ObjectReceiveStream[DataChunk]]:
        _internal_producer, chunks_stream = create_memory_object_stream[DataChunk](
            max_buffer_size=self._memory_constraints.numberOfChunks
        )  # for releasing the internal semaphore on consumption
        chunks_send, _internal_consumer = create_memory_object_stream[bytes](
            max_buffer_size=self._memory_constraints.numberOfChunks
        )

        async def send_current(buf):
            await chunks_send.send(bytes(buf))

        async def producer():
            buf = bytearray()
            async with (
                chunks_send,
                self._upper_stream
            ):
                it = self._upper_stream.__aiter__()
                while True:
                    raw = b''
                    with move_on_after(self._memory_constraints.timeBeforeFlush) as data_or_flush:
                        try:
                            raw = await it.__anext__()
                        except StopAsyncIteration:
                            break

                    if raw:
                        buf += raw
                    while len(buf) >= self._memory_constraints.chunkSize:
                        await send_current(buf[:self._memory_constraints.chunkSize])
                        buf = buf[self._memory_constraints.chunkSize:]

                    if data_or_flush.cancelled_caught and buf:
                        await send_current(buf)
                        buf = bytearray()

                if buf:
                    await send_current(buf)

        async def consumer():
            async with _internal_consumer, _internal_producer:
                async for chunk in _internal_consumer:
                    await _internal_producer.send(chunk)

        async with create_task_group() as tg:
            tg.start_soon(producer)
            tg.start_soon(consumer)
            yield chunks_stream



class ChunkedBytesToChunkObjects(AsyncContextManagerMixin):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes], chunks_in_mem: int):
        self._upper_stream = upper_stream
        self._chunks_in_mem = chunks_in_mem

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[ObjectReceiveStream[DataChunk]]:
        _internal_producer, chunks_stream = create_memory_object_stream[DataChunk](
            max_buffer_size=self._chunks_in_mem
        )

        async def consumer():
            index = 0
            offset = 0
            async with self._upper_stream, _internal_producer:
                async for chunk in self._upper_stream:
                    await _internal_producer.send(
                        DataChunk(
                            data=chunk,
                            index=index,
                            offset=offset,
                            integrity=None
                        )
                    )
                    offset += len(chunk)
                    index += 1

        async with create_task_group() as tg:
            tg.start_soon(consumer)
            yield chunks_stream


class CommonDataBufferAsyncProcessing(AsyncContextManagerMixin):

    def __init__(self, upper_stream: ObjectReceiveStream[bytes],
                 sync_processing_callable: Callable[[BaseModel], CommonDataBufferSyncProcessing], arguments: BaseModel, *,
                 memory_constraints: ChunkInMemoryConstraint | None = None, reset_event: Event | None = None):
        self._memory_constraints = memory_constraints or ChunkInMemoryConstraint()
        self._upper_stream = upper_stream
        self._reset_event = reset_event
        self._sync_processing_callable = sync_processing_callable
        self._arguments_construct_sync_process = arguments

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[ObjectReceiveStream[bytes]]:
        compressed_data_send, compressed_data_stream = create_memory_object_stream[bytes](
            max_buffer_size=self._memory_constraints.chunkSize * 2
        )
        async with (
            ChunkedBytes(compressed_data_stream, self._memory_constraints) as chunk_stream,
            anyio.create_task_group() as tg
        ):
            processed_data = bytearray()
            limiter = anyio.CapacityLimiter(1)

            async def process(processor, chunk: bytes) -> None:
                nonlocal processed_data
                processed_data += await to_thread.run_sync(processor, chunk, limiter=limiter)

            async def send_current(data):
                await compressed_data_send.send(data)

            async def while_no_reset(it):
                nonlocal processed_data

                processor: CommonDataBufferSyncProcessing = self._sync_processing_callable(self._arguments_construct_sync_process)
                print(processor)
                async with processor:
                    header = await to_thread.run_sync(processor.begin, limiter=limiter) or b''
                    await send_current(header)
                    while True:
                        if self._reset_event and self._reset_event.is_set():
                            print("Received signal to reset compressor, redoing")
                            return True

                        raw = b''
                        with move_on_after(self._memory_constraints.timeBeforeFlush) as data_or_flush:
                            try:
                                raw = await it.__anext__()
                            except StopAsyncIteration:  # async for exhaustion
                                break

                        if raw:
                            print("before processor, ", processor,  len(processed_data), len(raw))
                            await process(processor, raw)
                            print("after processor, ", processor, len(processed_data))
                        else:
                            print("flushing at the end")
                            processed_data += await to_thread.run_sync(processor.flush, limiter=limiter) or b''

                        while len(processed_data) >= self._memory_constraints.chunkSize:
                            print("processor buf is full", len(processed_data))
                            await send_current(processed_data[:self._memory_constraints.chunkSize])
                            processed_data = processed_data[self._memory_constraints.chunkSize:]

                        if data_or_flush.cancelled_caught and processed_data:
                            print("sending processor data", processor, len(processed_data))
                            await send_current(processed_data)
                            processed_data = bytearray()

                    processed_data += await to_thread.run_sync(processor.end, limiter=limiter) or b''
                    if processed_data:
                        print("SEND END", len(processed_data))
                        await send_current(processed_data)

                    return False

            async def compressor_main():
                nonlocal processed_data
                async with (
                    compressed_data_send,
                    self._upper_stream as chunks,
                ):
                    it = chunks.__aiter__()
                    while await while_no_reset(it):
                        pass

            tg.start_soon(compressor_main)
            yield chunk_stream


class ChunkedContentSender(AsyncContextManagerMixin):
    """This class takes into account minimal chunk management, with basic retry until chunk are well received
    We suppose nice bytes chunks are arriving, as these will be included "as-is" in produced chunks
    """

    def __init__(self, bytes_generator: ChunkedBytes | MemoryObjectReceiveStream[bytes],
                 global_constraints: TimedGlobalChunksConstraint, local_constraints: ContentTransferParameters):
        self._upper_stream = bytes_generator
        self._current_transfer_state = None
        self._global_constraints = global_constraints
        self._local_constraints = local_constraints

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._current_transfer_state = ContentTransferState(
            missingChunks=[],
            remainingTimeEstimation=None
        )
        chunks_send, chunks_stream = create_memory_object_stream[DataChunk]()
        sem = Semaphore(self._memory_constraints.numberOfChunks)

        async def producer():
            buf = bytearray()
            async with (
                chunks_send,
                self._upper_stream
            ):
                it = self._upper_stream.__aiter__()
                while True:
                    with move_on_after(self._global_constraints.maximumChunkTime.total_seconds()) as data_or_time_elapsed:
                        try:
                            raw = await it.__anext__()
                        except EndOfStream:
                            return

                    if data_or_time_elapsed.cancelled_caught:
                        mark_as_lost()
                    else:
                        buf += raw
                        while len(buf) >= self._memory_constraints.chunkSize:
                            await send_current(bytes(buf[:self._memory_constraints.chunkSize]))
                            buf = buf[self._memory_constraints.chunkSize:]

        async def watchdog_total_time():
            with move_on_after(self._global_constraints.maximumTotalTime.total_seconds()) as max_time:
                pass

            if max_time.cancelled_caught:
                logger.info("Cancelling task, max time elapsed")

        async with create_task_group() as tg:
            tg.start_soon(producer)
            tg.start_soon(watchdog_total_time)
            yield chunks_stream

        async with create_task_group() as tg:
            await tg.start(self.receive_peer_constraints)
            await tg.start(self.propose_chunk_parameters)
            tg.start_soon(self.send_chunks)
            yield self


import anyio


# anyio memory streams carry at most this many events before back-pressuring
_STREAM_BUFFER = 256


class ObservableChunkedFileClient():
    """
    Exactly like ChunkedFileClient but publishes ChunkEvents on a
    MemoryObjectStream that callers can read from.

    Usage:
        send, recv = anyio.create_memory_object_stream(max_buffer_size=256)
        client = ObservableChunkedFileClient(event_sink=send, cfg=cfg)

        async with anyio.create_task_group() as tg:
            tg.start_soon(client.upload, "file.bin")
            tg.start_soon(my_progress_listener, recv)
    """

    def __init__(
            self,
            event_sink,
            cfg,
    ) -> None:
        super().__init__(cfg)
        self._sink = event_sink

    async def _emit(self, event) -> None:
        try:
            await self._sink.send(event)
        except anyio.ClosedResourceError:
            pass

    async def _upload_chunk(
            self,
            client,
            chunk,
            results: list,
            lock: anyio.Lock,
    ) -> None:
        await self._emit(ChunkEvent(
            chunk_index=chunk.index,
            status=ChunkStatus.QUEUED,
            bytes_total=chunk.size,
        ))

        async with self._limiter:
            await self._emit(ChunkEvent(
                chunk_index=chunk.index,
                status=ChunkStatus.SENDING,
                bytes_total=chunk.size,
            ))
            try:
                result = await self._send_chunk_observable(client, chunk)
                await self._window.on_success()
                async with lock:
                    results[chunk.index] = result
                await self._emit(ChunkEvent(
                    chunk_index=chunk.index,
                    status=ChunkStatus.DONE,
                    bytes_total=chunk.size,
                    bytes_done=chunk.size,
                ))
            except (RetryError, httpx.HTTPStatusError, httpx.TransportError) as exc:
                await self._window.on_failure()
                await self._emit(ChunkEvent(
                    chunk_index=chunk.index,
                    status=ChunkStatus.FAILED,
                    bytes_total=chunk.size,
                    error=str(exc),
                ))
                raise RuntimeError(
                    f"Chunk {chunk.index} failed after all retries"
                ) from exc

    def _make_retrying_sender(self):
        """
        Wraps tenacity sender so we can emit RETRYING events between attempts.
        Tenacity's before_sleep hook receives the retry state, letting us
        forward attempt number and upcoming wait to the stream.
        """
        import functools
        from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type

        cfg = self.cfg
        sink = self._sink

        async def _before_sleep(retry_state):
            chunk: Chunk = retry_state.args[1]  # positional arg to send_chunk
            await sink.send(ChunkEvent(
                chunk_index=chunk.index,
                status=ChunkStatus.RETRYING,
                bytes_total=chunk.size,
                attempt=retry_state.attempt_number,
                error=str(retry_state.outcome.exception()),
            ))

        @retry(
            retry=retry_if_exception_type((httpx.TransportError, httpx.HTTPStatusError)),
            stop=stop_after_attempt(cfg.max_retries),
            wait=wait_exponential_jitter(
                initial=cfg.backoff_min,
                max=cfg.backoff_max,
                jitter=cfg.backoff_jitter,
            ),
            before_sleep=_before_sleep,
            reraise=True,
        )
        async def send_chunk(client: httpx.AsyncClient, chunk: Chunk) -> ChunkResult:
            response = await client.post(
                cfg.upload_url,
                content=chunk.data,
                headers={
                    "X-Chunk-Index": str(chunk.index),
                    "X-Chunk-Offset": str(chunk.offset),
                    "X-Chunk-Checksum": chunk.checksum,
                    "Content-Type": "application/octet-stream",
                },
                timeout=30.0,
            )
            response.raise_for_status()
            return ChunkResult(
                index=chunk.index,
                offset=chunk.offset,
                size=chunk.size,
                server_etag=response.headers.get("ETag", ""),
            )

        return send_chunk

    async def _send_chunk_observable(
            self, client, chunk
    ):
        sender = self._make_retrying_sender()
        return await sender(client, chunk)


if __name__ == '__main__':
    async def main() -> None:
        cfg = ClientConfig(
            chunk_size=512 * 1024,
            max_parallel_chunks=3,
            upload_url="http://localhost:8000/upload",
        )
        results = await upload_with_progress("large_file.bin", cfg)
        total_mb = sum(r.size for r in results) / 1e6
        print(f"\nDone â€” {len(results)} chunks, {total_mb:.2f} MB uploaded.")

    anyio.run(main)