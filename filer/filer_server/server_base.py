from contextlib import asynccontextmanager

from filer.filer_backend.backend_effectful import FilerBackend
from utils.custom_context_var import ContextVarWrapper


class GenericFilerServer(EffectfulBackend[HashType, UlidType], AsyncContextManagerMixin):

    def __init__(self, params: GenericBackendParams, internal_ez_thing):
        self._params = params
        self._write_limiter = anyio.CapacityLimiter(params.concurrentParallelWrites)
        self._read_limiter = anyio.CapacityLimiter(params.concurrentParallelReads)

    def register_backend(self, backend_type, backend_configuration):
        current_backends.append(...)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self.startup_metadata_report = IntegrityReport[HashType, UlidType]()
        if self._params.cacheMetadataAtStartup:
            self.startup_metadata_report = self.ensure_integrity(delete_bad=self._params.allowedDeletion)

        if self._params.throwIfNotExpected and self.startup_metadata_report.unexpectedItems:
            raise UnexpectedItems(self.startup_metadata_report.unexpectedItems)

        if self._params.throwIfNoFullIntegrity and self.startup_metadata_report.contentNotMatchingHashes:
            raise ContentNotMatchingHashes(self.startup_metadata_report.contentNotMatchingHashes)

        try:
            yield self.startup_metadata_report
        finally:
            if not self._params.allowedExternalModifications and self._params.allowedDeletion:  # this case redo a full integrity check
                final_report = self.ensure_integrity(delete_bad=self._params.allowedDeletion)
                for fname in final_report.unexpectedItems:
                    self.delete_content(fname)
                for hash in final_report.contentNotMatchingHashes:
                    self.delete_content(hash)


current_backends = ContextVarWrapper[list[EffectfulBackend[HashType, BackendFailure]]]('current_backends')
