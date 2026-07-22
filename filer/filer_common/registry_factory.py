from filer.filer_common.registry_fs import FsRegistryConfig, FsRegistryInContext
from filer.filer_common.registry_inmem import InMemRegistryInContext

from pydantic import BaseModel
from ulid import ULID

from datetime import datetime, timedelta


class InMemRegistryConfig(BaseModel):
    pass

class DbRegistryInContextConfig(BaseModel):
    pass

class DbRegistryWithSqlitePathConfig(BaseModel):
    dbFilename: str

class RemoteRegistryInContext(BaseModel):
    pass


KnownFilerRegistryParameters = FsRegistryConfig | InMemRegistryConfig | DbRegistryInContextConfig | DbRegistryWithSqlitePathConfig | RemoteRegistryInContext


class UlidWrapper(ULID):

    def __init__(self, s: str | None = None):
        if s:
            super().__init__(ULID.from_str(s).bytes)
        else:
            super().__init__()


class SimpleRegistryMetadataPydantic(BaseModel):
    dateBeginUpload: datetime
    dateEndUpload: datetime
    numberOfAccesses: int = 0


def FilerRegistryFor(registry_params: KnownFilerRegistryParameters):
    match registry_params:
        case FsRegistryConfig():
            return FsRegistryInContext[bytes, UlidWrapper, SimpleRegistryMetadataPydantic](
                params=registry_params,
                hash_type=bytes,
                ulid_type=UlidWrapper,
                metadata_type=SimpleRegistryMetadataPydantic
            )
        case DbRegistryInContextConfig():
            # don't import sqlalchemy dependency if not required / not installed
            from filer.filer_common.registry_db import SimpleRegistryMetadataSqlalchemy, DatabaseRegistryInContext
            return DatabaseRegistryInContext[bytes, SimpleRegistryMetadataSqlalchemy](
                hash_type=bytes,
                metadata_type=SimpleRegistryMetadataSqlalchemy
            )
        case DbRegistryWithSqlitePathConfig():
            from filer.filer_common.registry_db import SimpleRegistryMetadataSqlalchemy, SQLiteDatabaseRegistryCreateDbContext
            return SQLiteDatabaseRegistryCreateDbContext[bytes, SimpleRegistryMetadataSqlalchemy](
                DbRegistryWithSqlitePathConfig.dbFilename,
                hash_type=bytes,
                metadata_type=SimpleRegistryMetadataSqlalchemy
            )
        case InMemRegistryConfig():
            return InMemRegistryInContext[bytes, UlidWrapper, SimpleRegistryMetadataPydantic](
                hash_type=bytes,
                ulid_type=UlidWrapper,
                metadata_type=SimpleRegistryMetadataPydantic,
                keep_deleted_metadata=True
            )
        case _:
            raise NotImplementedError


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from filer.filer_common.registry_db import SimpleRegistryMetadataSqlalchemy, SQLiteDatabaseRegistryCreateDbContext
    from filer.filer_common.registry_protocol import SimpleListQueryRequest
    from baseimplems.date_utils import utc_now

    import anyio

    f1 = FilerRegistryFor(FsRegistryConfig(mock=True))
    f2 = FilerRegistryFor(DbRegistryInContextConfig())
    f3 = FilerRegistryFor(InMemRegistryConfig())

    async def main():
        async with f1:
            print(
                await f1.new_item(
                b't1',
                    SimpleRegistryMetadataPydantic(
                        dateBeginUpload=utc_now(),
                        dateEndUpload=utc_now()+timedelta(days=2),
                    ),
                    150
                )
            )
            print(await f1.list_items(SimpleListQueryRequest()))
            print(await f1.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))
            print(await f1.list_items_of_type(bytes, SimpleListQueryRequest()))

        async with (
            run_with_temporarily_persistent_mock_db_engine(echo=True),
            f2
        ):
            print(
                await f2.new_item(
                    b't2',
                    SimpleRegistryMetadataSqlalchemy(
                        date_begin_upload=utc_now(),
                        date_end_upload=utc_now() + timedelta(days=2),
                    ),
                    152
                )
            )
            print(await f2.list_items(SimpleListQueryRequest()))
            print(await f2.list_items_of_type(ULID, SimpleListQueryRequest()))
            print(await f2.list_items_of_type(bytes, SimpleListQueryRequest()))

        async with f3:
            print(
                await f3.new_item(
                    b't3',
                    SimpleRegistryMetadataPydantic(
                        dateBeginUpload=utc_now(),
                        dateEndUpload=utc_now() + timedelta(days=8),
                    ),
                    154
                )
            )
            print(await f3.list_items(SimpleListQueryRequest()))
            print(await f3.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))
            print(await f3.list_items_of_type(bytes, SimpleListQueryRequest()))

    anyio.run(main)
