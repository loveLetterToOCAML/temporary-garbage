from filer.base_exceptions import FilerSerialException, FilerConstraintType, OutOfConstraints, \
    AlreadyUploadingContent, NotExistingPlaceholder, NotExistingContent, ExpectedAgainstReality, PredicateType, \
    BadOffset
from filer.filer_backend.backend_failure import RegistryFailure, ExternalFailure, ExternalFailureType, BackendFailure
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackendWithContextManagement
from basetypes.implementation.dataformat.compression import CompressionAlgorithmInstance
from baseimplems.datastreams.constrained import StreamWatchguard, StreamConstraints
from filer.filer_backend.backend_remote import RemoteBackendInContextParameters
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters
from filer.filer_backend.content_integrity_cache import is_hash_valid_for_exn
from filer.filer_backend.backend_impl_sql import DbBackendInContextParameters
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters
from baseimplems.datastreams.stream_event import StreamEndReason
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend.interval_union import IntervalUnion
from filer.filer_backend.utils_exn import SerialException

from pydantic import BaseModel

from contextlib import asynccontextmanager
from anyio import create_task_group, Lock, CancelScope
from typing import AsyncIterator


class ConstraintsParameters(BaseModel):
    concurrentParallelWrites: int = 0x20
    concurrentParallelReads: int = 0x100
    maximumContentSize: int = 0x400000000  # 16 Gb max
    minimumContentSize: int = 0x40         # content less than 0x40 should not be uploaded in filer
    maximumSizeWrite: int = 0x1000000
    maximumSizeRead: int = 0x1000000
    fixedChunkSize: int = -1               # negative = no fixed size chunk required


# These are params fed to any backend constructor for its own "self-awareness", from an external trusted point of view
# There is no way whatsoever for a running backend to ensure these parameters are real
# Also this statement is true from an external point of view: a trust / authority relationship is required
# Or a "community" peer judgement which can state whether the claimed isolation is right (like an audit team)
class GenericBackendParameters(BaseModel):
    allowedRead: bool = True
    allowedWrite: bool = True
    allowedDeletion: bool = False


KnownFilerBackendParameters = FilerBackendInMemParameters | FilerBackendFsParameters | DbBackendInContextParameters | \
                              RemoteBackendInContextParameters

class ConstrainedBackendParameters(BaseModel):
    globalParameters: GenericBackendParameters = GenericBackendParameters()
    backendParameters: KnownFilerBackendParameters
    constraintParameters: ConstraintsParameters = ConstraintsParameters()

    compressDataAlgorithm: CompressionAlgorithmInstance | None = None
    compressThreshold: float = 0.8  # when compressed data size < compressThreshold * size (& compressDataAlgorithm is true), will store compressed

    uploadConstraints: StreamConstraints
    downloadConstraints: StreamConstraints


# TODO: check_final_content_hash_exn

"""
EffectfulConstrainedFilerBackend is the most complete piece of policies applied at generic backend level

Its role is both to ensure strict stream constraints are respected, as well as hashes are matching uploaded content
It also has the role of checking if compression would be useful and apply it
"""

class EffectfulConstrainedFilerBackend(
    EffectfulFilerBackendWithContextManagement[Hashed, BackendFailure],
    EffectfulBackend[Hashed, BackendFailure],
):
    def __init__(self, params: ConstrainedBackendParameters):
        self._params = params
        self._constraints = params.constraintParameters

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._current_bytes_uploaded_existing_content = 0
        self._total_transmitted_upload = 0
        self._current_max_placeholder_index = -1
        self._lock = Lock()

        self._total_transmitted_upload_success = 0
        self._current_bytes_expected = 0

        self._upload_guard = {}
        self._upload_stream = {}
        self._upload_poller = {}
        self._upload_end_event = {}
        self._upload_status = {}

        self._upload_stream_constraints = self._params.uploadConstraints

        from filer.filer_backend.backend_factory import FilerBackendFor
        self._internal = FilerBackendFor(self._params.backendParameters)
        if isinstance(self._internal, EffectfulFilerBackendWithContextManagement) or hasattr(self._internal, '__asynccontextmanager__'):
            async with (
                self._internal,
                create_task_group() as self._current_task_group
            ):
                yield
        else:
            async with create_task_group() as self._current_task_group:
                yield


    async def size_of_content_at_exn(self, locator: Hashed) -> int:
        return await self._internal.size_of_content_at_exn(locator)

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        if not self._params.globalParameters.allowedWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_UPLOAD
                )
            )

        if total_size > self._constraints.maximumContentSize:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_TOTAL_SIZE,
                    details=ExpectedAgainstReality[int](expectationType=PredicateType.INFERIOR, reference=self._constraints.maximumContentSize, got=total_size)
                )
            )

        if total_size < self._constraints.minimumContentSize:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_TOTAL_SIZE,
                    details=ExpectedAgainstReality[int](expectationType=PredicateType.SUPERIOR, reference=self._constraints.minimumContentSize, got=total_size)
                )
            )

        if placeholder_index < 0:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=locator,
                    placeholderIndex=placeholder_index
                )
            )

        if placeholder_index >= 0 and placeholder_index <= self._current_max_placeholder_index:
            raise FilerSerialException(
                AlreadyUploadingContent(
                    hashUploading=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )
        self._current_max_placeholder_index = placeholder_index  # increase the max placeholder index, which is supposed to auto-increment from filer server

        # Not handled here: only a filer server has the full view of backend init listing all, and registry containing sizes
        #if self._total_transmitted_upload + total_size > self._params.storageSize:
        #    raise FilerSerialException(
        #        NotEnoughSpaceRemaining(
        #            requestedSize=total_size,
        #            remainingSize=self._params.storageSize - self._total_transmitted_upload
        #        )
        #    )

        result = await self._internal.prepare_placeholder_for_hash_exn(locator, placeholder_index, total_size)
        if isinstance(result, RegistryFailure):
            raise result.originalException
        self._current_task_group.start_soon(self._safe_upload_monitoring, locator, placeholder_index, total_size)


    async def _safe_upload_monitoring(self, locator: Hashed, placeholder_index:int, total_size: int):
        # we are not supposed to know the sizes in backend: only the filer server should have the big picture
        # so we just do stats here
        async with self._lock:
            # placeholder_index = self._current_max_placeholder_index
            self._current_max_placeholder_index += 1
            self._total_transmitted_upload += total_size
            self._current_bytes_expected += total_size

        result = None
        try:
            result = await self._start_upload_monitoring(locator, placeholder_index)
        finally:
            with CancelScope(shield=True):
                await self.upload_terminate_for_hash_exn(locator, placeholder_index)
            #async with self._lock:
            #    if result:
            #        self._total_transmitted_upload_success += total_size
            #        self._current_bytes_uploaded_existing_content += total_size
            #    self._current_bytes_expected -= total_size


    async def _start_upload_monitoring(self, locator: Hashed, placeholder_index: int) -> bool:
        guard = StreamWatchguard(self._upload_stream_constraints, 'constrained-upload')
        self._upload_guard[(locator, placeholder_index)] = guard
        async with (
            guard as (remote_send_stream, external_stream_poller, external_stream_end),
            remote_send_stream,
        ):
            self._upload_stream[(locator, placeholder_index)] = remote_send_stream
            self._upload_poller[(locator, placeholder_index)] = external_stream_poller
            self._upload_end_event[(locator, placeholder_index)] = external_stream_end
            await external_stream_end.wait()  # this will be automatically set when guard task group will close
        status = self._upload_status[(locator, placeholder_index)] = guard.current_internal_state()
        print(status)
        return status.status.reason == StreamEndReason.END_OF_INPUT

    def _stop_stream(self, locator: Hashed, placeholder_index: int):
        if self._upload_end_event.get((locator, placeholder_index)):
            self._upload_end_event[(locator, placeholder_index)].set()

    def _stream_state(self, locator: Hashed, placeholder_index: int):
        if self._upload_poller.get((locator, placeholder_index)):
            return self._upload_poller[(locator, placeholder_index)].snapshot()

    def _stream_status(self, locator: Hashed, placeholder_index: int):
        if self._upload_status.get((locator, placeholder_index)):
            return self._upload_status[(locator, placeholder_index)]
        elif self._upload_poller.get((locator, placeholder_index)):
            return self._upload_poller[(locator, placeholder_index)].snapshot().status

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        if not self._params.globalParameters.allowedWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_UPLOAD
                )
            )

        size = len(data)
        if size > self._constraints.maximumSizeWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_CHUNK_SIZE
                )
            )

        if size < self._constraints.minimumSizeWrite:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_CHUNK_SIZE
                )
            )

        if placeholder_index < 0 or placeholder_index >= self._current_max_placeholder_index or \
                (locator, placeholder_index) not in self._expected_total_size_for:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=locator.hash,
                    placeholderIndex=placeholder_index
                )
            )

        if self._constraints.fixedChunkSize and size != self._constraints.fixedChunkSize and \
                ((offset % self._constraints.fixedChunkSize) != 0 or
                 self._expected_total_size_for[(locator, placeholder_index)] - offset >= self._constraints.fixedChunkSize):
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.FIXED_CHUNK_SIZE_EXPECTED
                )
            )

        size_written = await self._internal.upload_chunk_at(locator, placeholder_index, offset, data)

        interval = self._intervals_for_id[placeholder_index]
        data_slices = self._data_slices_per_id[placeholder_index]

        interval_tuple = (offset, min(offset + size, self._expected_total_size_for[(locator, placeholder_index)]))
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
            self._current_bytes_expected -= self._expected_total_size_for[placeholder_index]

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        increase_reserved_size = False
        print("GOING TERMINATION")
        if self._temporaryfiles_per_placeholder_index[placeholder_index].is_complete:
            await is_hash_valid_for_exn(locator, self._temporaryfiles_per_placeholder_index[placeholder_index].complete_data_gen_exn)
            # check_final_content_hash_exn(locator, self._temporaryfiles_per_placeholder_index[placeholder_index].complete_data_gen_exn)
            increase_reserved_size = True

        result = await self._internal.upload_terminate_at(locator, placeholder_index)
        if isinstance(result, BackendFailure):
            print("GOT FAILUER", result)
            raise result.originalException

        print("ICI", increase_reserved_size)
        if increase_reserved_size:  # we wall it after upload_terminate_at, which, if in error, won't cause reserved size increment
            self._current_bytes_uploaded_existing_content += self._expected_total_size_for[placeholder_index]

        # in case upload_terminate_at fails, this cleanup won't be called yet. We may retry upload termination few times
        # then the cleanup task will call _ensure_clean_termination_for_placeholder_called_exactly_once anyway
        self._ensure_clean_termination_for_placeholder_called_exactly_once(placeholder_index)


    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        if not self._constraints.globalParameters.allowedRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_DOWNLOAD
                )
            )

        if size > self._constraints.maximumSizeRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MAX_CHUNK_SIZE
                )
            )

        if size < self._constraints.minimumSizeRead:
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

        if offset >= sz:
            raise FilerSerialException(
                BadOffset(
                    inputHash=locator.hash,
                    askedOffset=offset,
                    dataSize=sz
                )
            )

        return await self._internal.download_chunk_for_hash_exn(locator, offset, size)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        if not self._params.globalParameters.allowedDelete:
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
            self._current_bytes_uploaded_existing_content -= sz

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        async for hash in self._internal.list_resources_reorganize_exn():
            yield hash

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulConstrainedFilerBackend exception',
                retryable=False
            )
        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulConstrainedFilerBackend::InternalError',
            retryable=False,
            originalException=exn
        )
