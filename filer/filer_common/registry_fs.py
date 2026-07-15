from __future__ import annotations

from filer.filer_common.registry_protocol import Registry, SimpleListQueryRequest, SimpleListQueryResponse, \
    RegistryInContext

from anyio import AsyncContextManagerMixin, Lock
from pydantic import BaseModel
from ulid import ULID

from typing import TypeVar


HashType = TypeVar('HashType', bound=str | bytes | BaseModel)
UlidType = TypeVar('UlidType')
MetadataType = TypeVar('MetadataType', bound=BaseModel)


class FsRegistry(Registry[HashType, ULID, MetadataType]):

    def __init__(self, filename: str, hash_type: type[HashType], metadata_type: type[MetadataType]):
        self._deleted = {}
        self._hash_type = hash_type
        self._ulid_type = ulid_type
        self._metadata_type = metadata_type

    async def hash_for_ulid_exn(self, ulid: UlidType) -> HashType | None:
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

    async def new_item(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0) -> UlidType:
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
        if item_type == self._hash_type:
            return SimpleListQueryResponse[HashType](
                results = self._hashes[request.offset: request.offset+request.limit],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        elif item_type == self._ulid_type:
            return SimpleListQueryResponse[UlidType](
                results = [self._ulids_for_hashes[h] for h in self._hashes[request.offset: request.offset+request.limit]],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        elif item_type == self._metadata_type:
            return SimpleListQueryResponse[MetadataType](
                results = [self._metadata_for_hashes[h] for h in self._hashes[request.offset: request.offset+request.limit]],
                hasMore = request.offset + request.limit < len(self._hashes),
            )
        else:
            raise NotImplementedError


class InMemRegistryInContext(RegistryInContext[HashType, UlidType, MetadataType], AsyncContextManagerMixin):

    def __init__(self, initial_metadata: dict[HashType, MetadataType] | None = None, initial_ulids: dict[HashType, UlidType] | None = None, *,
                 hash_type: type[HashType], ulid_type: type[UlidType], metadata_type: type[MetadataType]):
        reg = InMemRegistry[HashType, UlidType, MetadataType](
            initial_metadata=initial_metadata,
            initial_ulids=initial_ulids,
            hash_type=hash_type,
            ulid_type=ulid_type,
            metadata_type=metadata_type
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
        async with InMemRegistryInContext[bytes, Ulid, M](hash_type=bytes, ulid_type=Ulid, metadata_type=M) as mock:
            print(await mock.new_item(b'x', M(a=123)))
            print(await mock.new_item(b'y', M(b='metadata')))
            print(await mock.list_items(SimpleListQueryRequest(limit=1)))
            print(await mock.list_items_of_type(bytes, SimpleListQueryRequest()))
            print(await mock.list_items_of_type(Ulid, SimpleListQueryRequest()))

    anyio.run(test)
