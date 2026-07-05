from __future__ import annotations

from filer.filer_common.registry_protocol import Registry, SimpleListQueryRequest, SimpleListQueryResponse, \
    RegistryInContext
from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy, with_auto_session, \
    with_auto_session_kwargs
from baseimplems.persistence.model_utils.model_utils_common import TWithID, TWithBytesHash, TWithStringHash, WithID
from baseimplems.persistence.mixins import RepositoryMixin, commit_and_rollback_if_exception, BaseMixins
from filer.filer_common.registry_db.model import RegistryMetadataTable_for

from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from contextlib import asynccontextmanager
from typing import TypeVar, Type, Any


HashType = TypeVar('HashType')
MetadataType = TypeVar('MetadataType', bound=RepositoryMixin)


class DatabaseRegistry(Registry[HashType, ULID, MetadataType]):

    def __init__(self, *, metadata_type: TWithID, hash_type: TWithStringHash | TWithBytesHash | Type | None = None,
                 delete_metadata_info_on_delete: bool = False):
        self._metadata_type: RepositoryMixin = RegistryMetadataTable_for(metadata_type, hash_type)
        self._delete_metadata_info_on_delete = delete_metadata_info_on_delete
        self._internal_metadata_type = metadata_type
        self._hash_type = hash_type
        self._metadata_pydantic = metadata_type.pydantic_from_sqlalchemy()
        try:
            self._hash_pydantic = hash_type.pydantic_from_sqlalchemy()
        except:
            self._hash_pydantic = None

    @with_auto_session
    async def hash_for_ulid(self, ulid: ULID) -> HashType | None:
        result = await self._metadata_type.get_for(ulid=ulid)
        if result:
            return result.hash

    @with_auto_session
    async def ulid_for_hash(self, hash: HashType) -> ULID | None:
        result = await self._metadata_type.get_for(hash=hash)
        if result:
            return result.ulid

    @with_auto_session
    async def check_hash_and_ulid(self, hash: HashType, ulid: ULID) -> bool | None:
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
        return await self.old_metadata_for_hash(hash)

    @with_auto_session
    async def old_metadata_for_hash(self, hash: HashType) -> MetadataType | None:
        return (await self._internal_metadata_type.execute_query(
            select(self._internal_metadata_type, self._metadata_type) \
                .options(joinedload(self._metadata_type.metadata_instance)) \
                .join(self._metadata_type)
                .filter_by(hash=hash)
        )).scalar_one()

    @with_auto_session_kwargs
    async def new_item(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0, *, session: AsyncSession) -> ULID:
        already_there = await self._metadata_type.get_for(hash=hash)
        if already_there:
            raise Exception(f"Already known {hash} with ulid {already_there.ulid}")
        new_obj = await self._metadata_type.create(hash=hash, metadata_instance=item_metadata, size_of=size_of_data)
        await commit_and_rollback_if_exception(session)
        return new_obj.ulid

    @with_auto_session
    async def delete_item(self, hash: HashType) -> bool | None:
        already_there = await self._metadata_type.get_for(hash=hash)
        if not already_there:
            raise Exception(f"Metadata for hash {hash} does not exist, no deletion possible")
        if self._delete_metadata_info_on_delete:
            await already_there.delete()
        else:
            already_there.is_deleted = True

    @with_auto_session
    async def resolve_query(self, offset, limit, attr_func, includes_deleted: bool = False, includes_metadata: bool = False):
        if offset < 0 or limit <= 0:
            raise IndexError(offset)
        base_query = select(self._metadata_type)
        if not includes_deleted:
            base_query = base_query.filter_by(is_deleted=False)
        if includes_metadata:
            base_query = base_query.options(joinedload(self._metadata_type.metadata_instance)).join(self._internal_metadata_type)
        results = await self._metadata_type.execute_query(
            base_query.offset(offset).limit(limit)
        )
        count = await self._metadata_type.execute_query(
            select(func.count()).select_from(self._metadata_type)
        )
        res = [attr_func(mt) for mt in results.scalars()]
        final = SimpleListQueryResponse[self._metadata_pydantic | self._hash_pydantic | self._hash_type | ULID](
            results = res,
            hasMore = offset + limit < count.scalar_one(),
        )
        return final

    @with_auto_session
    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[Any]:  # not able to state the dynamic type self._metadata_type
        return await self.resolve_query(request.offset, request.limit, lambda x: self._metadata_pydantic.model_validate(x.metadata_instance), request.includesDeleted, True)

    @with_auto_session
    async def list_items_of_type(self, item_type: type[HashType | str | MetadataType | Any], request: SimpleListQueryRequest) -> \
            SimpleListQueryResponse[HashType | str | MetadataType | Any]:
        includes_mt = False
        if item_type == self._hash_type:
            attr_func = lambda x: self._hash_pydantic and self._hash_pydantic.model_validate(x.hash) or x.hash
        elif item_type == self._metadata_type:
            includes_mt = True
            attr_func = lambda x: x
        elif item_type == str or item_type == ULID:
            attr_func = lambda x: x.ulid
        elif item_type == self._internal_metadata_type:
            includes_mt = True
            attr_func = lambda x: self._metadata_pydantic.model_validate(x.metadata_instance)
        else:
            raise NotImplementedError

        return await self.resolve_query(request.offset, request.limit, attr_func, request.includesDeleted, includes_mt)


class DatabaseRegistryInContext(RegistryInContext[HashType, ULID, MetadataType]):

    def __init__(self, *, metadata_type: TWithID, hash_type: TWithStringHash | TWithBytesHash | Type | None = None,
                 delete_metadata_info_on_delete: bool = False):
        reg = DatabaseRegistry[HashType, MetadataType](
            metadata_type=metadata_type, hash_type=hash_type, delete_metadata_info_on_delete=delete_metadata_info_on_delete
        )
        super().__init__(reg, self._ensure_current_db)

    @asynccontextmanager
    async def _ensure_current_db(self):
        async with (
            run_within_sqlalchemy() as db,
            db,
        ):
            yield


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from sqlalchemy.testing.schema import mapped_column
    from sqlalchemy.orm import Mapped, joinedload
    from sqlalchemy import Integer, select, func
    import anyio


    class M(WithID, *BaseMixins):
        __tablename__ = 'M'
        a: Mapped[int] = mapped_column(Integer)


    async def test():
        async with (
            run_with_temporarily_persistent_mock_db_engine(echo=False),
            DatabaseRegistryInContext[bytes, M](hash_type=bytes, metadata_type=M) as mock
        ):
            print(await mock.new_item(b'x', M(a=123)))
            print(await mock.new_item(b'y', M(a=999)))
            #print(await mock.list_items(SimpleListQueryRequest(limit=1)))
            print(await mock.list_items_of_type(bytes, SimpleListQueryRequest()))
            print(await mock.list_items_of_type(str, SimpleListQueryRequest()))
            await mock.delete_item(b'y')
            print(await mock.metadata_for_hash(b'y'))
            print(await mock.old_metadata_for_hash(b'y'))
            print(await mock.list_items(SimpleListQueryRequest()))
            print(await mock.list_items(SimpleListQueryRequest(includesDeleted=True)))
            #print(await mock.new_item(b'y', M(a=9994523)))
        print(await mock.new_item(b'x', M(a=123)))

    anyio.run(test)
