from __future__ import annotations

from filer.filer_common.registry_protocol import Registry, SimpleListQueryRequest, SimpleListQueryResponse, \
    RegistryInContext

from sortedcontainers import SortedDict
from pydantic import BaseModel
from anyio import Lock

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
                 hash_type: type[HashType], ulid_type: type[UlidType], metadata_type: type[MetadataType], keep_deleted_metadata: bool = False):
        self._metadata_for_hashes = initial_metadata or {}
        self._ulids_for_hashes = {h: ulid for h, ulid in (initial_ulids or {}).items() if h in initial_metadata}
        self._ulids_for_hashes = {h: UlidType() for h in self._metadata_for_hashes if h not in self._ulids_for_hashes}
        self._hashes_for_ulids = {u: h for h, u in self._metadata_for_hashes.items()}
        if len(self._hashes_for_ulids) != len(self._ulids_for_hashes):
            raise Exception(f"Ulids and hashes should be unique {len(self._hashes_for_ulids)} {len(self._ulids_for_hashes)}")
        self._hashes = list(self._metadata_for_hashes.keys())
        self._hashes_ok = SortedDict()
        self._sizes_for_hash = {}
        self._deleted = set()
        self._hash_type = hash_type
        self._ulid_type = ulid_type
        self._metadata_type = metadata_type
        self._keep_deleted_metadata = keep_deleted_metadata
        self._lock = Lock()

    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        return self._hashes_for_ulids.get(ulid)

    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        return self._ulids_for_hashes.get(hash)

    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        u = self._ulids_for_hashes.get(hash)
        if u is None:
            return
        return u == ulid

    async def size_for_hash(self, hash: HashType) -> int | None:
        return self._sizes_for_hash.get(hash)

    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:
        return hash in self._deleted or self._metadata_for_hashes.get(hash)

    async def old_metadata_for_hash(self, hash: HashType) -> MetadataType | None:
        return self._metadata_for_hashes.get(hash)

    async def new_item(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0) -> UlidType:
        if hash in self._ulids_for_hashes:
            raise Exception(f"Already known {hash} with ulid {self._ulids_for_hashes}")
        async with self._lock:
            ulid = self._ulid_type()
            if ulid in self._hashes_for_ulids:
                raise Exception(f"Generated ulid {ulid} already in internal state, should not happen")
            self._hashes_for_ulids[ulid] = hash
            self._ulids_for_hashes[hash] = ulid
            self._metadata_for_hashes[hash] = item_metadata
            self._hashes.append(hash)
            self._hashes_ok[hash] = True
            self._sizes_for_hash[hash] = size_of_data
            if hash in self._deleted:
                self._deleted = self._deleted.difference({hash})
        return ulid

    async def delete_item(self, hash: HashType) -> bool | None:
        if hash not in self._ulids_for_hashes:
            return None

        async with self._lock:
            del self._ulids_for_hashes[hash]
            del self._hashes_ok[hash]
            if not self._keep_deleted_metadata:
                del self._metadata_for_hashes[hash]
                del self._sizes_for_hash[hash]
            self._deleted.add(hash)

    async def resolve_query(self, offset, limit, hash_t, for_h):
        if offset < 0 or limit <= 0 or offset >= len(hash_t):
            raise IndexError(offset)
        async with self._lock:
            return SimpleListQueryResponse(
                items = [for_h[h] if for_h else h for h in hash_t[offset: offset+limit]],
                total = len(hash_t),
                hasMore = offset + limit < len(hash_t),
            )

    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[MetadataType]:
        if request.sizeInferiorTo is not None or request.sizeSuperiorTo is not None:
            raise NotImplementedError
        if request.includesDeleted:
            return await self.resolve_query(request.offset, request.limit, self._hashes, self._metadata_for_hashes)
        return await self.resolve_query(request.offset, request.limit, self._hashes_ok.keys(), self._metadata_for_hashes)

    async def list_items_of_type(self, item_type: type[HashType | UlidType | MetadataType], request: SimpleListQueryRequest) -> \
            SimpleListQueryResponse[HashType | UlidType | MetadataType]:
        if request.includesDeleted:
            hash_source = self._hashes
        else:
            hash_source = self._hashes_ok.keys()

        if item_type == self._hash_type:
            data_source = None
        elif item_type == self._ulid_type:
            data_source = self._ulids_for_hashes
        elif item_type == self._metadata_type:
            data_source = self._metadata_for_hashes
        else:
            raise NotImplementedError

        return await self.resolve_query(request.offset, request.limit, hash_source, data_source)


class InMemRegistryInContext(RegistryInContext[HashType, UlidType, MetadataType]):

    def __init__(self, initial_metadata: dict[HashType, MetadataType] | None = None, initial_ulids: dict[HashType, UlidType] | None = None, *,
                 hash_type: type[HashType], ulid_type: type[UlidType], metadata_type: type[MetadataType], keep_deleted_metadata: bool = False):
        reg = InMemRegistry[HashType, UlidType, MetadataType](
            initial_metadata=initial_metadata,
            initial_ulids=initial_ulids,
            hash_type=hash_type,
            ulid_type=ulid_type,
            metadata_type=metadata_type,
            keep_deleted_metadata=keep_deleted_metadata,
        )
        super().__init__(reg)


if __name__ == '__main__':
    import random
    import anyio

    class Ulid:
        def __init__(self):
            self.r = random.randint(0, 200)

        def __eq__(self, other):
            return self.r == other.r

        def __hash__(self):
            return self.r

        def __repr__(self):
            return f"{self.r}"

    class M(BaseModel):
        a: int = 1
        b: str = 'b'

    async def test():
        async with InMemRegistryInContext[bytes, Ulid, M](hash_type=bytes, ulid_type=Ulid, metadata_type=M, keep_deleted_metadata=True) as mock:
            print(await mock.new_item(b'x', M(a=123)))
            print(await mock.new_item(b'y', M(b='metadata')))
            print(await mock.list_items(SimpleListQueryRequest(limit=1)))
            print(await mock.list_items_of_type(bytes, SimpleListQueryRequest()))
            print(await mock.list_items_of_type(Ulid, SimpleListQueryRequest()))
            await mock.delete_item(b'y')
            print(await mock.metadata_for_hash(b'y'))
            print(await mock.old_metadata_for_hash(b'y'))
            print(await mock.list_items(SimpleListQueryRequest()))
            print(await mock.list_items(SimpleListQueryRequest(includesDeleted=True)))
            print(await mock.new_item(b'y', M(b='metadataother')))
            print(await mock.old_metadata_for_hash(b'y'))
        print(await mock.new_item(b'x', M(a=123)))

    anyio.run(test)
