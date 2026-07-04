from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from filer.filer_common.registry_protocol import Registry, SimpleListQueryRequest, SimpleListQueryResponse, \
    RegistryInContext
from baseimplems.persistence.model_utils.model_utils_common import TWithID, TWithBytesHash, TWithStringHash
from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy, with_auto_session, \
    with_auto_session_kwargs
from filer.filer_common.registry_db.model import RegistryMetadataTable_for
from baseimplems.persistence.mixins import RepositoryMixin, commit_and_rollback_if_exception

from anyio import AsyncContextManagerMixin, Lock
from contextlib import asynccontextmanager
from pydantic import BaseModel

from typing import TypeVar, Type

HashType = TypeVar('HashType')
MetadataType = TypeVar('MetadataType', bound=RepositoryMixin)



class DatabaseRegistry(Registry[HashType, str, MetadataType]):

    def __init__(self, *, metadata_type: TWithID, hash_type: TWithStringHash | TWithBytesHash | Type | None = None,
                 delete_metadata_info_on_delete: bool = False):
        self._metadata_type: RepositoryMixin = RegistryMetadataTable_for(metadata_type, hash_type)
        self._delete_metadata_info_on_delete = delete_metadata_info_on_delete

    @with_auto_session
    async def hash_for_ulid(self, ulid: str) -> HashType | None:
        result = await self._metadata_type.get_for(ulid=ulid)
        if result:
            return result.hash

    @with_auto_session
    async def ulid_for_hash(self, hash: HashType) -> str | None:
        result = await self._metadata_type.get_for(hash=hash)
        if result:
            return result.ulid

    @with_auto_session
    async def check_hash_and_ulid(self, hash: HashType, ulid: str) -> bool | None:
        result = await self._metadata_type.get_for(hash=hash)
        if not result:
            return None
        return result.ulid == ulid

    @with_auto_session
    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:
        result = await self._metadata_type.get_for(hash=hash)
        if not result:
            return
        if result.is_deleted:
            return True
        return result

    @with_auto_session
    async def old_metadata_for_hash(self, hash: HashType) -> MetadataType | None:
        return await self._metadata_type.get_for(hash=hash)

    @with_auto_session_kwargs
    async def new_item(self, hash: HashType, item_metadata: MetadataType, *, session: AsyncSession) -> str:
        already_there = await self._metadata_type.get_for(hash=hash)
        if already_there:
            raise Exception(f"Already known {hash} with ulid {already_there.ulid}")
        new_obj = await self._metadata_type.create(hash=hash, metadata_instance=item_metadata)
        await commit_and_rollback_if_exception(session)
        return new_obj.ulid

    @with_auto_session
    async def delete_item(self, hash: HashType) -> bool | None:
        already_there = await self._metadata_type.get_for(hash=hash)
        if already_there:
            return
        if self._delete_metadata_info_on_delete:
            already_there.delete()
        else:
            already_there.is_deleted = True

    @with_auto_session
    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[MetadataType]:
        if request.offset < 0 or request.limit <= 0:
            raise IndexError(request.offset)
        return SimpleListQueryResponse[MetadataType](
            results = [self._metadata_for_hashes[h] for h in  self._metadata_type.get_for(hash=hash)],
            hasMore = request.offset + request.limit < len(self._hashes),
        )

    @with_auto_session
    async def list_items_of_type(self, item_type: type[HashType | str | MetadataType], request: SimpleListQueryRequest) -> \
            SimpleListQueryResponse[HashType | str | MetadataType]:
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

class DatabaseRegistryInContext(RegistryInContext[HashType, str, MetadataType]):

    def __init__(self, *, metadata_type: TWithID, hash_type: TWithStringHash | TWithBytesHash | Type | None = None):
        self._metadata_type = RegistryMetadataTable_for(metadata_type, hash_type)
        reg = DatabaseRegistry[HashType, MetadataType](
            metadata_type, hash_type
        )
        super().__init__(reg, self._ensure_current_db)

    @asynccontextmanager
    async def _ensure_current_db(self):
        async with (
            run_within_sqlalchemy() as db,
            db,
        ):
            yield

    async def hash_for_ulid(self, ulid: str) -> HashType | None:
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
        return ulid

    async def delete_item(self, hash: HashType) -> bool | None:
        if hash not in self._ulids_for_hashes:
            return None

        async with Lock():
            del self._ulids_for_hashes[hash]
            del self._metadata_for_hashes[hash]
            self._deleted[hash] = True

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
