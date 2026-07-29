from filer.base_exceptions import FilerSerialException, FilerConstraintType, OutOfConstraints, \
    AlreadyUploadingContent, NotExistingPlaceholder, NotExistingContent, ExpectedAgainstReality, PredicateType, \
    BadOffset
from filer.filer_backend.backend_failure import RegistryFailure, ExternalFailure, ExternalFailureType, BackendFailure
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackendWithContextManagement, \
    EffectfulFilerBackend
from basetypes.implementation.dataformat.compression import CompressionAlgorithmInstance
from baseimplems.datastreams.constrained import StreamWatchguard, StreamConstraints
from filer.filer_backend.backend_remote import RemoteBackendInContextParameters
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters
from filer.filer_backend.content_integrity_cache import EnsureContentIntegrity
from filer.filer_backend.backend_impl_sql import DbBackendInContextParameters
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters
from filer.filer_backend.interval_union_bytes import BytesIntervalUnion
from baseimplems.datastreams.stream_event import StreamEndReason
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend.utils_exn import SerialException

from anyio import create_task_group, Lock, CancelScope, AsyncContextManagerMixin
from pydantic import BaseModel

from typing import AsyncIterator, Any, TypeVar
from contextlib import asynccontextmanager


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')


class ConstraintsParameters(BaseModel):
    concurrentParallelWrites: int = 0x20
    concurrentParallelReads: int = 0x100
    maximumContentSize: int = 0x400000000  # 16 Gb max
    minimumContentSize: int = 0x40         # content less than 0x40 should not be uploaded in filer
    maximumSizeWrite: int = 0x1000000
    minimumSizeRead: int = 0x1000
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
    EffectfulFilerBackend[Hashed, ExternalResourceLocatorType, BackendFailure],
    EffectfulBackend[Hashed, BackendFailure],
    EnsureContentIntegrity[Hashed],
    AsyncContextManagerMixin
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
        self._intervals_for_id: dict[tuple[Hashed, int], BytesIntervalUnion] = {}
        self._uploaded_size_for: dict[tuple[Hashed, int], int] = {}
        self._expected_total_size_for: dict[tuple[Hashed, int], int] = {}

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


    @property
    def _effectful_backend(self) -> EffectfulBackend[ExternalResourceLocatorType, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: ExternalResourceLocatorType) -> Hashed | None:
        return self._internal.hash_from_resource_locator(locator)

    def resource_locator_from_hash(self, hash: Hashed) -> ExternalResourceLocatorType:
        return self._internal.resource_locator_from_hash(hash)


    async def size_of_content_at_exn(self, locator: ExternalResourceLocatorType) -> int:
        return await self._internal._effectful_backend.size_of_content_at_exn(locator)

    async def prepare_placeholder_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, total_size: int):
        hash : Hashed = self.hash_from_resource_locator(locator)
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
                    inputHash=hash.hash,
                    placeholderIndex=placeholder_index
                )
            )

        if placeholder_index >= 0 and placeholder_index <= self._current_max_placeholder_index:
            raise FilerSerialException(
                AlreadyUploadingContent(
                    hashUploading=hash.hash,
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

        result = await self._internal.prepare_placeholder_for_hash_exn(hash, placeholder_index, total_size)
        if isinstance(result, RegistryFailure):
            raise result.originalException

        self._intervals_for_id[(hash, placeholder_index)] = BytesIntervalUnion(total_size, store_data=False)
        self._expected_total_size_for[(hash, placeholder_index)] = total_size
        self._uploaded_size_for[(hash, placeholder_index)] = 0
        self._current_task_group.start_soon(self._safe_upload_monitoring, hash, placeholder_index, total_size)


    async def _safe_upload_monitoring(self, hash: Hashed, placeholder_index:int, total_size: int):
        # we are not supposed to know the sizes in backend: only the filer server should have the big picture
        # so we just do stats here
        async with self._lock:
            # placeholder_index = self._current_max_placeholder_index
            self._current_max_placeholder_index += 1
            self._total_transmitted_upload += total_size
            self._current_bytes_expected += total_size

        result = None
        try:
            result = await self._start_upload_monitoring(hash, placeholder_index)
        finally:
            with CancelScope(shield=True):
                await self.upload_terminate_for_hash_exn(hash, placeholder_index)
            #async with self._lock:
            #    if result:
            #        self._total_transmitted_upload_success += total_size
            #        self._current_bytes_uploaded_existing_content += total_size
            #    self._current_bytes_expected -= total_size


    async def _start_upload_monitoring(self, hash: Hashed, placeholder_index: int) -> bool:
        guard = StreamWatchguard(self._upload_stream_constraints, 'constrained-upload')
        self._upload_guard[(hash, placeholder_index)] = guard
        async with (
            guard as (remote_send_stream, external_stream_poller, external_stream_end),
            remote_send_stream,
        ):
            self._upload_stream[(hash, placeholder_index)] = remote_send_stream
            self._upload_poller[(hash, placeholder_index)] = external_stream_poller
            self._upload_end_event[(hash, placeholder_index)] = external_stream_end
            await external_stream_end.wait()  # this will be automatically set when guard task group will close
        status = self._upload_status[(hash, placeholder_index)] = guard.current_internal_state()
        print(status)
        return status.status.reason == StreamEndReason.END_OF_INPUT

    def _stop_stream(self, hash: Hashed, placeholder_index: int):
        if self._upload_end_event.get((hash, placeholder_index)):
            self._upload_end_event[(hash, placeholder_index)].set()

    def _stream_state(self, hash: Hashed, placeholder_index: int):
        if self._upload_poller.get((hash, placeholder_index)):
            return self._upload_poller[(hash, placeholder_index)].snapshot()

    def _stream_status(self, hash: Hashed, placeholder_index: int):
        if self._upload_status.get((hash, placeholder_index)):
            return self._upload_status[(hash, placeholder_index)]
        elif self._upload_poller.get((hash, placeholder_index)):
            return self._upload_poller[(hash, placeholder_index)].snapshot().status

    async def upload_chunk_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int, offset: int, data: bytes) -> int:
        hash: Hashed = self.hash_from_resource_locator(locator)
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
                (hash, placeholder_index) not in self._expected_total_size_for:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=hash.hash,
                    placeholderIndex=placeholder_index
                )
            )

        if self._constraints.fixedChunkSize and size != self._constraints.fixedChunkSize and \
                ((offset % self._constraints.fixedChunkSize) != 0 or
                 self._expected_total_size_for[(hash, placeholder_index)] - offset >= self._constraints.fixedChunkSize):
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.FIXED_CHUNK_SIZE_EXPECTED
                )
            )

        _ = await self._internal.upload_chunk_for_hash_exn(hash, placeholder_index, offset, data)

        interval = self._intervals_for_id[(hash, placeholder_index)]
        written = interval.union_from(offset, data)
        self._uploaded_size_for[(hash, placeholder_index)] = written

        if interval.number_parts > self._params.maxIntervalParts:
            raise SerialException(
                ExternalFailure(
                    externalFailureType=ExternalFailureType.TriggeredSecurity,
                    humanMessage=f"Too much parts encountered during upload: "
                                 f"{interval.number_parts} instead of max {self._params.maxIntervalParts} expected",
                )
            )

        if interval.is_complete:
            await self.upload_terminate_at_exn(locator, placeholder_index)
        return written

    def _ensure_clean_termination_for_placeholder_called_exactly_once(self, hash: Hashed, placeholder_index: int):
        if (hash, placeholder_index) in self._intervals_for_id:
            self._current_bytes_expected -= self._expected_total_size_for[(hash, placeholder_index)]
            self._stop_stream(hash, placeholder_index)
            del self._upload_guard[(hash, placeholder_index)]
            del self._upload_stream[(hash, placeholder_index)]
            del self._upload_poller[(hash, placeholder_index)]
            del self._upload_end_event[(hash, placeholder_index)]
            del self._upload_status[(hash, placeholder_index)]
            del self._intervals_for_id[(hash, placeholder_index)]
            del self._uploaded_size_for[(hash, placeholder_index)]
            del self._expected_total_size_for[(hash, placeholder_index)]

    def _download_backend(self) -> EffectfulFilerBackend[Hashed, ExternalResourceLocatorType, BackendFailure]:  # for EnsureContentIntegrity
        return self._internal

    async def upload_terminate_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int):
        hash: Hashed = self.hash_from_resource_locator(locator)
        # TODO: we had to terminate upload (so one must really ensure the target internal is a cached fast backend
        # because currently we have to use the download method of the backend to check the integrity
        # otherwise one should ensure packe order so that there is no need to keep data in memory for computing the
        # hash in parallel. One can also note this may not solve the compression problem, while in the current case it's
        # not optimized but ok
        result = await self._internal.upload_terminate_for_hash_exn(hash, placeholder_index)
        if isinstance(result, BackendFailure):
            print("GOT FAILUER", result)
            raise result.originalException

        print("GOING TERMINATION")
        if self._intervals_for_id[(hash, placeholder_index)].is_complete:
            await self.is_hash_valid_for_exn(hash)

            # we call it after upload_terminate_at, which, if in error, won't cause reserved size increment
            self._current_bytes_uploaded_existing_content += self._expected_total_size_for[(hash, placeholder_index)]
            self._total_transmitted_upload_success += self._expected_total_size_for[(hash, placeholder_index)]

        # in case upload_terminate_at fails, this cleanup won't be called yet. We may retry upload termination few times
        # then the cleanup task will call _ensure_clean_termination_for_placeholder_called_exactly_once anyway
        self._ensure_clean_termination_for_placeholder_called_exactly_once(hash, placeholder_index)


    async def download_chunk_from_exn(self, locator: ExternalResourceLocatorType, offset: int, size: int) -> bytes:
        if not self._params.globalParameters.allowedRead:
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

        hash: Hashed = self.hash_from_resource_locator(locator)
        sz = await self.size_of_content_at_exn(locator)
        if isinstance(sz, BackendFailure) or sz is None:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=hash.hash,
                )
            )

        if offset >= sz:
            raise FilerSerialException(
                BadOffset(
                    inputHash=hash.hash,
                    askedOffset=offset,
                    dataSize=sz
                )
            )

        if size < self._constraints.minimumSizeRead and sz - offset >= self._constraints.minimumSizeRead:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.MIN_CHUNK_SIZE
                )
            )

        return await self._internal.download_chunk_for_hash_exn(hash, offset, size)

    async def delete_resource_at_exn(self, locator: ExternalResourceLocatorType, placeholder_index: int = -1):
        hash: Hashed = self.hash_from_resource_locator(locator)
        if not self._params.globalParameters.allowedDelete:
            raise FilerSerialException(
                OutOfConstraints(
                    failedConstraint=FilerConstraintType.NO_DELETION
                )
            )

        if placeholder_index >= self._current_max_placeholder_index:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=hash.hash,
                    placeholderIndex=placeholder_index
                )
            )

        await self._internal.delete_content_exn(hash, placeholder_index)

        # same remark here: in case delete_resource_at_exn fails, this cleanup won't be called yet. We may retry deletion and
        # termination few times then the cleanup task will call _ensure_clean_termination_for_placeholder_called_exactly_once anyway
        if placeholder_index >= 0:
            self._ensure_clean_termination_for_placeholder_called_exactly_once(hash, placeholder_index)
        else:
            sz = self.size_of_content_at_exn(locator)
            self._current_bytes_uploaded_existing_content -= sz

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[ExternalResourceLocatorType]:
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
