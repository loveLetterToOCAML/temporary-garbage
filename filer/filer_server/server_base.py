from __future__ import annotations

from filer.filer_backend.backend_proxy_constrained import GenericBackendParameters, ConstrainedBackendParameters
from filer.base_exceptions import NotExistingContent, FilerSerialException, AlreadyUploadedContent
from filer.filer_backend.backend_protocol import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.backend_failure import BackendFailure, RegistryFailure
from filer.filter_registry.registry import FilerRegistryParameters
from filer.filer_server.server_chain import HashableWithBytesRepr
from filer.filer_backend.backend_effectful import IntegrityReport
from basetypes.implementation.dataformat.hashed import Hashed

from anyio import AsyncContextManagerMixin
from pydantic import BaseModel

from contextlib import asynccontextmanager
from typing import AsyncIterator, TypeVar


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')
BackendFailureType = TypeVar('BackendFailureType')
UlidType = TypeVar('UlidType')
HashType = TypeVar('HashType', bound=HashableWithBytesRepr)


class FilerServerInitParameters(BaseModel):
    # in case of no external modification: there is no live check when not in cache, and all data that is in the
    # repository not matching an expected content hash of ulid is destroyed at the end (if deletion is allowed)
    allowedExternalModifications: bool = False
    cacheMetadataAtStartup: bool = True
    throwIfNotExpected: bool = True
    throwIfNoFullIntegrity: bool = False
    onlyCheckIntegrityAtDownloadTime: bool = True


class FilerServerParameters(BaseModel):
    globalParameters: GenericBackendParameters  # filer server is still a backend, and can be configured to restrict what can be done on it independently of linked backends
    initParameters: FilerServerInitParameters | None  # if none, one can consider the backend is not persistent and no metadata import is performed
    backendParameters: ConstrainedBackendParameters
    registryParameters: FilerRegistryParameters

"""
EffectfulFilerServer = base component that ensures coherence between a fast-access registry and a backend
It handles upload and download, within the constraints that are specified per-server
It also initiates registry population and ensure coherence of data, either at the beginning or per download (depending
on configuration, as checking the integrity of all data everytime the component start can be (very) costly)

This class is type agnostic, but uses a simple metadata type which only relates to counting data accesses and measures times
The EffectfulFilerServerExternal will implement more complex metadata storage as managing external tickets authenticating
external users when performing actions
"""
class EffectfulFilerServer(
    EffectfulBackend[HashType, BackendFailure | RegistryFailure],
    EffectfulFilerBackend[HashType, HashType, BackendFailure | RegistryFailure],
    AsyncContextManagerMixin
):
    @property
    def _effectful_backend(self) -> EffectfulBackend[HashType, BackendFailure | RegistryFailure]:
        return self

    def hash_from_resource_locator(self, locator: HashType) -> HashType | None:
        return locator

    def resource_locator_from_hash(self, hash: HashType) -> HashType:
        return hash


    def __init__(self, server_params: FilerServerParameters):
        self._global_params = server_params
        self._init_parameters = self._global_params.initParameters
        self._backend_params = server_params.backendParameters
        self._registry_params = server_params.registryParameters

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._backend = FilerBackendFor(self._backend_params)
        self._registry = FilerRegistryFor(self._registry_params)
        async with (
            self._backend,
            self._registry,
        ):
            yield


    async def size_of_content_at_exn(self, locator: HashType) -> int:
        sz = await self._registry.size_for_hash_exn(hash)
        if sz:
            return sz
        if self._init_parameters and self._init_parameters.allowedExternalModifications:  # in this case perform a dynamic recheck
            sz = await self._backend.size_of_content_at_exn(locator)
            await self._registry.new_item_exn(locator, new_metadata, sz)
        if not sz:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash
                )
            )
        return sz

    async def _ensure_not_existing(self, locator: Hashed):
        existing_ulid = await self._registry.ulid_for_hash_exn(locator)
        if existing_ulid:
            raise FilerSerialException(
                AlreadyUploadedContent(
                    existingUlid=existing_ulid,
                    hashAttempted=locator.hash
                )
            )

    async def _ensure_existing(self, locator: Hashed):
        existing_md = await self._registry.metadata_for_hash_exn(locator)
        if not existing_md or existing_md is True:
            raise FilerSerialException(
                NotExistingContent(
                    inputHash=locator.hash,
                    hasExisted=existing_md is True
                )
            )

    async def prepare_placeholder_at_exn(self, locator: Hashed, placeholder_index: int, total_size: int):
        await self._ensure_not_existing(locator)
        await self._backend.prepare_placeholder_at_exn(locator, placeholder_index, total_size)

    async def upload_chunk_at_exn(self, locator: Hashed, placeholder_index: int, offset: int, data: bytes) -> int:
        await self._ensure_not_existing(locator)
        await self._backend.upload_chunk_at_exn(locator, placeholder_index, offset, data)

    async def upload_terminate_at_exn(self, locator: Hashed, placeholder_index: int):
        await self._ensure_not_existing(locator)
        await self._backend.upload_terminate_at_exn(locator, placeholder_index)
        # TODO: check the terminate is ok (hash & size match)
        upload_ok = True
        if upload_ok:
            sz = await self._backend.size_of_content_at_exn(locator)
            await self._registry.new_item_exn(locator, new_metadata, sz)

    async def download_chunk_from_exn(self, locator: Hashed, offset: int, size: int) -> bytes:
        await self._ensure_existing(locator)
        return await self._backend.download_chunk_from_exn(locator, offset, size)

    async def delete_resource_at_exn(self, locator: Hashed, placeholder_index: int = -1):
        if placeholder_index >= 0:
            await self._backend.delete_resource_at_exn(locator, placeholder_index)
        else:
            await self._ensure_existing(locator)
            await self._backend.delete_resource_at_exn(locator, -1)
            await self._registry.delete_item_exn(locator)

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[Hashed]:
        for hash in self._backend._list_resources_reorganize_exn():
            yield hash

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        return self._backend.exception_to_registry_failure(exn)

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        if self._init_parameters:
            allowed_deletion = self._global_params.globalParameters.allowedDeletion

            self.startup_metadata_report = IntegrityReport[HashType, UlidType]()
            if self._init_parameters.cacheMetadataAtStartup:
                self.startup_metadata_report = self.ensure_integrity(delete_bad=allowed_deletion)

            if self._init_parameters.throwIfNotExpected and self.startup_metadata_report.unexpectedItems:
                raise UnexpectedItems(self.startup_metadata_report.unexpectedItems)

            if self._init_parameters.throwIfNoFullIntegrity and self.startup_metadata_report.contentNotMatchingHashes:
                raise ContentNotMatchingHashes(self.startup_metadata_report.contentNotMatchingHashes)

        try:
            yield self.startup_metadata_report
        finally:
            if self._init_parameters and not self._init_parameters.allowedExternalModifications and allowed_deletion:  # this case redo a full integrity check
                final_report = self.ensure_integrity(delete_bad=allowed_deletion)
                for fname in final_report.unexpectedItems:
                    self.delete_content(fname)
                for hash in final_report.contentNotMatchingHashes:
                    self.delete_content(hash)
