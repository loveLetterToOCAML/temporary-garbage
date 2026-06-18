# Default pure python async implem of chunks send / recv strongly constrained by time
from anyio.abc import ObjectReceiveStream
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel

from basetypes.implementation.dataformat.chunk import ContentTransferParameters, ContentTransferState, \
    TimedGlobalChunksConstraint, DataChunk

from anyio import AsyncContextManagerMixin, create_task_group, create_memory_object_stream, Semaphore, move_on_after, \
    EndOfStream

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncGenerator, Self


# these are to move upper, at interaction layer so that these are globally defined after interaction between 2 parts
content_transfer_timed_constraints = ContextVar[TimedGlobalChunksConstraint]('global_chunk_constraints')
content_transfer_parameters = ContextVar[ContentTransferParameters]('transfer_parameters')



# 16 Mb memory usage by default
class ChunkInMemoryConstraint(BaseModel):
    chunkSize: int = 0x10000
    numberOfChunks: int = 0x100
    timeBeforeFlush: float = 0.1


class ChunkedBytes(AsyncContextManagerMixin):
    """ Simple memory caching to craft chunks from raw bytes
    """

    # we choose MemoryObjectReceiveStream and not simpler one because of the context management
    # also one should ensure the upper_stream cannot send more than too much data
    def __init__(self, upper_stream: ObjectReceiveStream[bytes],
                 memory_constraints: ChunkInMemoryConstraint = ChunkInMemoryConstraint()):
        self._upper_stream = upper_stream
        self._memory_constraints = memory_constraints

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        _internal_producer, chunks_stream = create_memory_object_stream[bytes]()  # for releasing the internal semaphore on consumption
        chunks_send, _internal_consumer = create_memory_object_stream[bytes](
            max_buffer_size=self._memory_constraints.numberOfChunks
        )
        sem = Semaphore(self._memory_constraints.numberOfChunks)

        async def send_current(buf):
            await sem.acquire()
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
                        print("buf is full", len(buf))
                        await send_current(buf[:self._memory_constraints.chunkSize])
                        buf = buf[self._memory_constraints.chunkSize:]

                    if data_or_flush.cancelled_caught and buf:
                        print("delay elapsed, sending remaning buf", len(buf))
                        await send_current(buf)
                        buf = bytearray()

                if buf:
                    await send_current(buf)
            print("LEAVING SEND")


        async def consumer():
            async with _internal_consumer, _internal_producer:
                async for chunk in _internal_consumer:
                    sem.release()
                    await _internal_producer.send(chunk)

        async with create_task_group() as tg:
            tg.start_soon(producer)
            tg.start_soon(consumer)
            yield chunks_stream


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