from random import randint
from typing import AsyncIterator

from pydantic import BaseModel

from baseimplems.datastreams.stream_event import StreamEvent, base_event_from
from baseimplems.date_utils import utc_now
from basetypes.implementation.dataformat.hashed import Hashed
from filer.base_exceptions import FilerSerialException, NotEnoughSpaceRemaining, AlreadyUploadedContent, \
    OutOfSpaceConstraints, FilerConstraintType, OutOfConstraints, AlreadyUploadingContent, \
    NotExistingPlaceholderForUpload, NotExistingContent
from filer.filer_backend.backend_failure import RegistryFailure, ExternalFailure, ExternalFailureType, BackendFailure
from filer.filer_backend.backend_impl_inmem import check_final_content_hash_exn
from filer.filer_backend.interval_union import IntervalUnion
from filer.filer_backend.utils_exn import SerialException



class EffectParams(BaseModel):
    concurrentParallelWrites: int = 0x40
    concurrentParallelReads: int = 0x100
    maximumContentSize: int = 0x400000000  # 16 Gb max
    minimumContentSize: int = 0x40         # content less than 0x40 should not be uploaded in filer
    maximumSizeWrite: int = 0x1000000
    maximumSizeRead: int = 0x1000000


# These are params fed to any backend constructor for its own "self-awareness", from an external trusted point of view
# There is no way whatsoever for a running backend to ensure these parameters are real
# Also this statement is true from an external point of view: a trust / authority relationship is required
# Or a "community" peer judgement which can state whether the claimed isolation is right (like an audit team)
class GenericBackendParams(BaseModel):
    allowedRead: bool = True
    allowedWrite: bool = True
    allowedDeletion: bool = False
    # in case of no external modification: there is no live check when not in cache, and all data that is in the
    # repository not matching an expected content hash of ulid is destroyed at the end (if deletion is allowed)
    allowedExternalModifications: bool = False
    cacheMetadataAtStartup: bool = True
    throwIfNotExpected: bool = True
    throwIfNoFullIntegrity: bool = False
    onlyCheckIntegrityAtDownloadTime: bool = True

    effectParams: EffectParams = EffectParams()

    compressDataAlgorithm: CompressionAlgorithmInstance | None = None
    compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size, will store compressed




# TODO: check_final_content_hash_exn


class EffectfulContrainedBackend(EffectfulBackend[Hashed, BackendFailure]):

    def __init__(self, params):
        self._params = params
        self._current_size_max = 0
        self._current_placeholder_index = 0

    async def size_of_content_at_exn(self, locator: Hashed) -> int:
        return await self._internal.size_of_content_at_exn(locator)

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        if not self._params.allowedWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_UPLOAD
                )
            )

        if total_size > self._params.maximumContentSize:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE
                )
            )

        if total_size < self._params.minimumContentSize:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_TOTAL_SIZE
                )
            )

        if placeholder_index >= 0 and placeholder_index < self._current_placeholder_index:
            raise FilerSerialException(
                AlreadyUploadingContent(
                    hashUploading=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )

        if self._current_size_max + total_size > self._params.storageSize:
            raise FilerSerialException(
                NotEnoughSpaceRemaining(
                    requestedSize=total_size,
                    remainingSize=self._params.storageSize - self._current_size_max
                )
            )

        result = await self._internal.prepare_placeholder_at(locator, self._current_placeholder_index)
        if isinstance(result, RegistryFailure):
            raise result.originalException
        self._current_task_group.start_soon(self._safe_upload_monitoring, locator, total_size)

    async def _safe_upload_monitoring(self, locator: Hashed, total_size: int):
        async with self._lock:
            placeholder_index = self._current_placeholder_index
            self._current_placeholder_index += 1
            self._current_size_max += total_size

        result = None
        try:
            result = await self._start_upload_monitoring(locator, placeholder_index)
        finally:
            async with self._lock:
                if result:
                    self._current_size += total_size
                self._current_size_max -= total_size

    async def _start_upload_monitoring(self, locator: Hashed, placeholder_index: int):
        async with (
            StreamWithConstraints(self._upload_stream_constraints) as \
                (remote_send_stream, remote_send_constraint_intent, remote_receive_constraint_info),
                remote_send_stream,
                remote_send_constraint_intent,
                remote_receive_constraint_info
        ):
            self._base_stream_event[(locator, placeholder_index)] = StreamEvent(
                name='constrainedDownload',
                index=placeholder_index,
                randomId=randint(0, 0xffffffff),
                **base_event_from(utc_now())
            )
            self._upload_stream[(locator, placeholder_index)] = remote_send_stream
            self._constraint_intents_for[(locator, placeholder_index)] = remote_send_constraint_intent

            # this can act as the main loop, while the stream is not closed it means upload continues
            async for last_constraint in remote_receive_constraint_info:
                # we only need the last constraint to be up to date
                self._last_constraint_for[(locator, placeholder_index)] = last_constraint


    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        if not self._params.allowedWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_UPLOAD
                )
            )

        size = len(data)
        if size > self._params.maximumSizeWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_CHUNK_SIZE
                )
            )

        if size < self._params.minimumSizeWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_CHUNK_SIZE
                )
            )

        if placeholder_index < 0 or placeholder_index >= self._current_placeholder_index:
            raise FilerSerialException(
                NotExistingPlaceholderForUpload(
                    inputHash=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )

        if self._params.fixedChunkSize and size != self._params.fixedChunkSize and \
                ((offset % self._params.fixedChunkSize) != 0 or
                 self._expected_total_size_for[placeholder_index] - offset >= self._params.fixedChunkSize):
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.FIXED_CHUNK_SIZE_EXPECTED
                )
            )

        size_written = await self._internal.upload_chunk_at(locator, placeholder_index, offset, data)

        interval = self._intervals_for_id[placeholder_index]
        data_slices = self._data_slices_per_id[placeholder_index]

        interval_tuple = (offset, min(offset + size, self._expected_total_size_for[placeholder_index]))
        intersection: IntervalUnion = interval.intersect(*interval_tuple)
        bytes_updated = 0
        for start, end in intersection.intervals:
            if (start, end) in data_slices:
                del data_slices[(start, end)]
                interval.delete(start, end)
                bytes_updated += start - end

        intersection_diff: IntervalUnion = interval.intersect_difference(*interval_tuple)
        for start, end in intersection_diff.intervals:
            data_slices[(start, end)] = True
        interval.add(*interval_tuple)
        self._uploaded_size_for[placeholder_index] += intersection_diff.actual_filled + bytes_updated
        written = intersection_diff.actual_filled + bytes_updated

        if interval.number_parts > self._params.maxIntervalParts:
            raise SerialException(
                ExternalFailure(
                    externalFailureType=ExternalFailureType.TriggeredSecurity,
                    humanMessage=f"Too much parts encountered during upload: "
                                 f"{self._temporaryfiles_per_placeholder_index[placeholder_index].number_parts} "
                                 f"instead of max {self._params.maxIntervalParts} expected",
                )
            )

        if self._temporaryfiles_per_placeholder_index[placeholder_index].is_complete:
            await self.upload_terminate_at_exn(locator, placeholder_index)
        return written


    def _ensure_clean_termination_for_placeholder_called_exactly_once(self, placeholder_index):
        if placeholder_index in self._temporaryfiles_per_placeholder_index:
            del self._temporaryfiles_per_placeholder_index[placeholder_index]
            self._current_size_max -= self._expected_total_size_for[placeholder_index]

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        increase_reserved_size = False
        if self._temporaryfiles_per_placeholder_index[placeholder_index].is_complete:
            check_final_content_hash_exn(locator, self._temporaryfiles_per_placeholder_index[placeholder_index].complete_data_gen_exn)
            increase_reserved_size = True

        result = await self._internal.upload_terminate_at(locator, placeholder_index)
        if isinstance(result, BackendFailure):
            raise result.originalException

        if increase_reserved_size:  # we wall it after upload_terminate_at, which, if in error, won't cause reserved size increment
            self._current_size += self._expected_total_size_for[placeholder_index]

        # in case upload_terminate_at fails, this cleanup won't be called yet. We may retry upload termination few times
        # then the cleanup task will call _ensure_clean_termination_for_placeholder_called_exactly_once anyway
        self._ensure_clean_termination_for_placeholder_called_exactly_once(placeholder_index)


    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        if not self._params.allowedRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_DOWNLOAD
                )
            )

        if size > self._params.maximumSizeRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_CHUNK_SIZE
                )
            )

        if size < self._params.minimumSizeRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_CHUNK_SIZE
                )
            )

        sz = await self._internal.size_for_hash(locator)
        if isinstance(sz, BackendFailure) or sz is None:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash,
                )
            )

        return await self._internal.download_chunk_for_hash_exn(locator, offset, size)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        if not self._params.allowedDelete:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_DELETION
                )
            )

        if placeholder_index >= self._current_placeholder_index:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )

        await self._internal.delete_resource_at_exn(locator, placeholder_index)

        if placeholder_index >= 0:
            self._ensure_clean_termination_for_placeholder_called_exactly_once(placeholder_index)
        else:
            sz = self.size_of_content_at_exn(locator)
            self._current_size -= sz

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        for hash in self._internal._list_resources_reorganize_exn():
            yield hash

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulConstrainedBackend exception',
                retryable=False
            )
        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulConstrainedBackend::InternalError',
            retryable=False,
            originalException=exn
        )
