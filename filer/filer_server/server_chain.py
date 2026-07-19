from __future__ import annotations

from filer.base_exceptions import FilerSerialException, AlreadyUploadedContent, MultiplePydanticFilerException, \
    NotExistingPlaceholderForUpload
from filer.filer_server.server_base import FilerServerParameters, BackendFailureType
from filer.filer_backend.backend_protocol import EffectfulBackend
from filer.filer_backend.backend_failure import BackendFailure

from anyio import AsyncContextManagerMixin
from pydantic import BaseModel

from typing import TypeVar, AsyncIterator, Protocol
from contextlib import asynccontextmanager


class HashableWithBytesRepr(Protocol):
    def __hash__(self) -> int:
        ...

    def __eq__(self, other) -> bool:
        ...

    def __bytes__(self) -> bytes:
        ...


HashType = TypeVar('HashType', bound=HashableWithBytesRepr)


class FilerServerChainParameters(BaseModel):
    fasterServerParameters: FilerServerParameters
    slowerServerParameters: FilerServerParameters | FilerServerChainParameters  # always construct from slower to faster


class EffectfulFilerServerChain(EffectfulBackend[HashType, BackendFailure], AsyncContextManagerMixin):

    def __init__(self, params: FilerServerChainParameters):
        self._faster_params = params.fasterServerParameters
        self._slower_params = params.slowerServerParameters

    # for getting size and downloading content, we return the first that has the information available, if any (or last exception)
    # we could merge exceptions, TODO
    async def size_of_content_at_exn(self, hash: HashType) -> int | None:
        try:
            return await self._faster.size_for_hash_exn(hash)
        except:
            return await self._slower.size_for_hash_exn(hash)

    async def download_chunk_from_exn(self, hash: HashType, offset: int, size: int) -> bytes:
        try:
            return await self._faster.download_chunk_for_hash_exn(hash, offset, size)
        except:
            return await self._slower.download_chunk_for_hash_exn(hash, offset, size)

    async def _check_placeholder_valid_exn(self, hash: HashType, placeholder_index: int):
        if (hash, placeholder_index) in self._aborted_placeholders or (hash, placeholder_index) in self._successful_placeholders:
            raise FilerSerialException(
                AlreadyUploadedContent(existingUlid=None, hashAttempted=bytes(hash))
            )

    async def prepare_placeholder_at_exn(self, hash: HashType, placeholder_index: int, total_size: int):
        await self._check_placeholder_valid_exn(hash, placeholder_index)

        exns = []
        async def perform_for(filer_server):
            try:
                await filer_server.prepare_placeholder_for_hash_exn(hash, placeholder_index, total_size)
                return True
            except Exception as exn:
                exns.append(exn)

        ph1 = await perform_for(self._faster)
        ph2 = await perform_for(self._slower)

        self._prepared_placeholders_for[(hash, placeholder_index)] = (ph1, ph2)
        if not ph1 and not ph2:
            raise FilerSerialException(
                MultiplePydanticFilerException(
                    exceptions=exns
                )
            )

    async def _check_placeholder_created_exn(self, hash: HashType, placeholder_index: int):
        if (hash, placeholder_index) not in self._prepared_placeholders_for:
            raise FilerSerialException(
                NotExistingPlaceholderForUpload(
                    inputHash=hash.hash,
                    placeholderIndex=placeholder_index
                )
            )

    async def upload_chunk_at_exn(self, hash: HashType, placeholder_index: int, offset: int, data: bytes) -> int:
        await self._check_placeholder_valid_exn(hash, placeholder_index)
        await self._check_placeholder_created_exn(hash, placeholder_index)

        ph1, ph2 = self._prepared_placeholders_for[(hash, placeholder_index)]
        exns = []

        async def perform_for(filer_server):
            try:
                return await filer_server.upload_chunk_for_hash_exn(hash, placeholder_index, offset, data)
            except Exception as exn:
                exns.append(exn)
                # if any upload part fail, we abort the full upload without retrying for now
                try:
                    await filer_server.upload_terminate_for_hash_exn(hash, placeholder_index)
                except Exception as exn:
                    exns.append(exn)

        written1 = await perform_for(self._faster) if ph1 else None
        written2 = await perform_for(self._slower) if ph2 else None
        ph1 = written1 is not None
        ph2 = written2 is not None

        self._prepared_placeholders_for[(hash, placeholder_index)] = (ph1, ph2)
        if not ph1 and not ph2:
            self._aborted_placeholders.add((hash, placeholder_index))
            raise FilerSerialException(
                MultiplePydanticFilerException(
                    exceptions=exns
                )
            )
        return min(written1, written2) if written1 and written2 else max(written1 or 0, written2 or 0)

    async def upload_terminate_at_exn(self, hash: HashType, placeholder_index: int):
        await self._check_placeholder_valid_exn(hash, placeholder_index)
        await self._check_placeholder_created_exn(hash, placeholder_index)

        ph1, ph2 = self._prepared_placeholders_for[(hash, placeholder_index)]
        exns = []

        async def perform_for(filer_server):
            try:
                return await filer_server.upload_terminate_for_hash_exn(hash, placeholder_index)
            except Exception as exn:
                exns.append(exn)

        if ph1:
            await perform_for(self._faster)
        if ph2:
            await perform_for(self._slower)

        if not ph1 and not ph2:
            self._aborted_placeholders.add((hash, placeholder_index))
            raise FilerSerialException(
                MultiplePydanticFilerException(
                    exceptions=exns
                )
            )
        self._successful_placeholders.add((hash, placeholder_index))

    async def delete_resource_at_exn(self, hash: HashType, placeholder_index: int = -1):
        try:
            return await self._faster.delete_content_exn(hash, placeholder_index)
        except Exception as exn1:
            try:
                return await self._slower.delete_content_exn(hash, placeholder_index)
            except Exception as exn2:
                raise FilerSerialException(
                    MultiplePydanticFilerException(
                        exceptions=[exn1, exn2]
                    )
                )

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[HashType]:
        accumulated_hashes = set()
        async for rsrc in self._faster.list_resources_reorganize():
            if rsrc not in accumulated_hashes:
                yield rsrc
                accumulated_hashes.add(rsrc)
        async for rsrc in self._slower.list_resources_reorganize():
            if rsrc not in accumulated_hashes:
                yield rsrc
                accumulated_hashes.add(rsrc)

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailureType:
        raise exn

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self._faster = FilerServerFor(self._faster_params)
        self._slower = FilerServerFor(self._slower_params)
        self._prepared_placeholders_for = {}
        self._aborted_placeholders = set()
        self._successful_placeholders = set()
        async with (
            self._slower,
            self._faster
        ):
            yield self
