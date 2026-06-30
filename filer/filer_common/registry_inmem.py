from __future__ import annotations

from filer.filer_common.registry_protocol import Registry, SimpleListQueryRequest, SimpleListQueryResponse
from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin, Lock
from pydantic import BaseModel

from typing_extensions import AsyncIterable
from contextlib import asynccontextmanager
from functools import wraps
from typing import TypeVar


HashType = TypeVar('HashType')
UlidType = TypeVar('UlidType')
MetadataType = TypeVar('MetadataType', bound=BaseModel)


# Registry intends to be infinite append-only (almost) objects, meaning there is no need to handle context management
# there is no context or async context management to implement there, but for the sake of mocking, we still provide
# a basic registry type which is only related to a scope below. This will not be done for DB or external cloud related
# backend. The in-mem and filesystem ones are special since these could be used for temporary mock / live functional
# verifications

class InMemRegistry(Registry[HashType, UlidType, MetadataType]):

    def __init__(self, initial_metadata: dict[HashType, MetadataType] | None = None, initial_ulids: dict[HashType, UlidType] | None = None, *,
                 hash_type: type[HashType] | None = None, ulid_type: type[UlidType] | None = None, metadata_type: type[MetadataType] | None = None
                 ):
        self._metadata_for_hashes = initial_metadata or {}
        self._ulids_for_hashes = {h: ulid for h, ulid in (initial_ulids or {}).items() if h in initial_metadata}
        self._ulids_for_hashes = {h: UlidType() for h in self._metadata_for_hashes if h not in self._ulids_for_hashes}
        self._hashes_for_ulids = {u: h for h, u in self._metadata_for_hashes.items()}
        if len(self._hashes_for_ulids) != len(self._ulids_for_hashes):
            raise Exception(f"Ulids and hashes should be unique {len(self._hashes_for_ulids)} {len(self._ulids_for_hashes)}")
        self._hashes = list(self._metadata_for_hashes.keys())
        self._deleted = {}
        self._hash_type = hash_type
        self._ulid_type = ulid_type
        self._metadata_type = metadata_type

    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        return self._hashes_for_ulids.get(ulid)

    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        return self._ulids_for_hashes.get(hash)

    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        u = self._ulids_for_hashes.get(hash)
        if u is None:
            return
        return u == ulid

    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:
        return self._metadata_for_hashes.get(hash) or self._deleted.get(hash)

    async def new_item(self, hash: HashType, item_metadata: MetadataType) -> UlidType:
        if hash in self._ulids_for_hashes:
            raise Exception(f"Already known {hash} with ulid {self._ulids_for_hashes}")
        async with Lock():
            ulid = self._ulid_type()
            if ulid in self._hashes_for_ulids:
                raise Exception(f"Generated ulid {ulid} already in internal state, should not happen")
            self._hashes_for_ulids[ulid] = hash
            self._ulids_for_hashes[hash] = ulid
            self._metadata_for_hashes[hash] = item_metadata
            self._hashes.append(hash)
            print(self._hashes_for_ulids)
        return ulid

    async def delete_item(self, hash: HashType) -> bool | None:
        if hash not in self._ulids_for_hashes:
            return None

        async with Lock():
            del self._ulids_for_hashes[hash]
            del self._metadata_for_hashes[hash]
            self._deleted[hash] = True

    async def preload_metadata(self) -> int:
        return 0

    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[MetadataType]:
        if request.offset < 0 or request.limit <= 0 or request.offset >= len(self._hashes):
            raise IndexError(request.offset)
        return SimpleListQueryResponse[MetadataType](
            results = [self._metadata_for_hashes[h] for h in self._hashes[request.offset: request.offset+request.limit]],
            hasMore = request.offset + request.limit < len(self._hashes),
        )

    async def list_items_of_type(self, item_type: type[HashType | UlidType | MetadataType], request: SimpleListQueryRequest) -> \
            SimpleListQueryResponse[HashType | UlidType | MetadataType]:
        if item_type == HashType:
            return SimpleListQueryResponse[HashType](
                results = self._hashes[request.offset: request.offset+request.limit],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        elif item_type == UlidType:
            return SimpleListQueryResponse[UlidType](
                results = [self._ulids_for_hashes[h] for h in self._hashes[request.offset: request.offset+request.limit]],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        elif item_type == MetadataType:
            return SimpleListQueryResponse[MetadataType](
                results = [self._metadata_for_hashes[h] for h in self._hashes[request.offset: request.offset+request.limit]],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        else:
            raise NotImplementedError


def guarded(func):
    @wraps(func)
    async def guard(self, *args, **kwargs):
        if not self._async_context_active:
            raise NotInAsyncContextManager(func.__name__, 'InMemRegistryInContext')
        return await func(self, *args, **kwargs)
    return guard


class InMemRegistryInContext(Registry[HashType, UlidType, MetadataType], AsyncContextManagerMixin):

    def __init__(self, initial_metadata: dict[HashType, MetadataType] | None = None, initial_ulids: dict[HashType, UlidType] | None = None, *,
                 hash_type: type[HashType] | None = None, ulid_type: type[UlidType] | None = None, metadata_type: type[MetadataType] | None = None
                 ):
        self._internal_registry = InMemRegistry[HashType, UlidType, MetadataType](
            initial_metadata=initial_metadata,
            initial_ulids=initial_ulids,
            hash_type=hash_type,
            ulid_type=ulid_type,
            metadata_type=metadata_type
        )
        self._async_context_active = False

    @guarded
    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        return await self._internal_registry.hash_for_ulid(ulid)

    @guarded
    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        return await self._internal_registry.ulid_for_hash(hash)

    @guarded
    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        return await self._internal_registry.check_hash_and_ulid(hash, ulid)

    @guarded
    async def metadata_for_hash(self, hash: HashType) -> MetadataType | None:
        return await self._internal_registry.metadata_for_hash(hash)

    @guarded
    async def new_item(self, hash: HashType, item_metadata: MetadataType) -> UlidType:
        return await self._internal_registry.new_item(hash, item_metadata)

    @guarded
    async def delete_item(self, hash: HashType) -> bool | None:  # responsibility remains to the implementer to handle soft-delete
        return await self._internal_registry.delete_item(hash)

    @guarded
    async def preload_metadata(self) -> int:  # `init` method, returns the number of metadata loaded
        return await self._internal_registry.preload_metadata()

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterable[InMemRegistryInContext]:
        try:
            self._async_context_active = True
            yield self
        finally:
            self._async_context_active = False


if __name__ == '__main__':

    import random

    class Ulid:
        def __init__(self):
            self.r = random.randint(0, 200)

        def __eq__(self, other):
            return self.r == other.r

        def __hash__(self):
            return self.r

        def __repr__(self):
            return f"{self.r}"

    async def test():
        async with InMemRegistryInContext[bytes, str, str](ulid_type=Ulid) as mock:
            print(await mock.new_item(b'x', 'metadata1'))
            print(await mock.new_item(b'y', 'metadata2'))

    import anyio

    anyio.run(test)
