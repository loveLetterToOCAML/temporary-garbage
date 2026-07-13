from __future__ import annotations

from baseimplems.anyio_utils import NotInAsyncContextManager, AsyncContextManagerDependencyNotEntered, run_within
from baseimplems.persistence.sqlalchemy_persist import sqlalchemy_db_engine
from baseimplems.contextvar_utils import ContextVarWrapper

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from anyio import AsyncContextManagerMixin, Event, Lock

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import wraps


class SqlalchemyBaseHandler(AsyncContextManagerMixin):

    def __init__(self, create_tables: bool = True, expire_on_commit: bool = False):
        self._create_tables = create_tables
        self._expire_on_commit = expire_on_commit
        self._ready = Event()
        self._lock = Lock()
        self._session_factory = None
        self._engine = None
        self._session = None

    async def force_schema_update(self):
        if not self._engine:
            raise NotInAsyncContextManager('force_schema_update', 'SqlalchemyBaseHandler')
        async with self._engine.begin() as conn:
            from baseimplems.persistence.mixins import MainSqlalchemyBase
            await conn.run_sync(MainSqlalchemyBase.metadata.create_all)

    async def _ensure_schema(self):
        async with self._lock:  # ensure we go there once at most
            if self._ready.is_set():
                return
            if self._create_tables:
                async with self._engine.begin() as conn:
                    # we may find better and more explicit way of doing later, but from now keep the main base in mixins
                    # so that any classes that inherits the mixin also get declared as a valid base
                    from baseimplems.persistence.mixins import MainSqlalchemyBase
                    await conn.run_sync(MainSqlalchemyBase.metadata.create_all)
            self._ready.set()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        if not self._session_factory:
            raise NotInAsyncContextManager('session', 'SqlalchemyBaseHandler')
        await self._ensure_schema()

        async with self._session_factory() as self._session:
            prev = current_sqlalchemy_session.set(self._session)
            try:
                yield self._session
            finally:
                current_sqlalchemy_session.reset(prev)
                from baseimplems.persistence.mixins import commit_and_rollback_if_exception
                await commit_and_rollback_if_exception(self._session)

    @property
    def current_session(self) -> AsyncSession:
        if not self._session_factory:
            raise NotInAsyncContextManager('current_session', 'SqlalchemyBaseHandler')
        return self._session

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterator[SqlalchemyBaseHandler]:
        try:
            self._engine: AsyncEngine = sqlalchemy_db_engine.get()
        except LookupError:
            raise AsyncContextManagerDependencyNotEntered('sqlalchemy_db_engine', 'AsyncEngine','SqlalchemyBaseHandler::__asynccontextmanager__')
        await self._ensure_schema()
        self._session_factory = async_sessionmaker(self._engine, expire_on_commit=self._expire_on_commit)
        prev = sqlalchemy_base.set(self)
        try:
            yield self
        finally:
            sqlalchemy_base.reset(prev)
            await self._engine.dispose()
            self._session_factory = None
            self._engine = None


sqlalchemy_base = ContextVarWrapper[SqlalchemyBaseHandler]('sqlalchemy_base')
run_within_sqlalchemy = run_within(SqlalchemyBaseHandler, sqlalchemy_base, reenter_context=True)

# we do not enforce it as the current_session property, since we allow a dedicated run_within this object
current_sqlalchemy_session = ContextVarWrapper[AsyncSession]('sqlalchemy_session', run_within_proposal='run_within_session')
run_within_session = run_within(AsyncSession, current_sqlalchemy_session)


def with_auto_session(f):
    @wraps(f)
    async def sub(*args, **kwargs):
        async with sqlalchemy_base.session():
            return await f(*args, **kwargs)
    return sub

def with_auto_session_kwargs(f):
    @wraps(f)
    async def sub(*args, **kwargs):
        async with sqlalchemy_base.session() as session:
            return await f(*args, **kwargs, session=session)
    return sub

def with_auto_session_kwargs_gen(f):
    @wraps(f)
    async def sub(*args, **kwargs):
        async with sqlalchemy_base.session() as session:
            async for data in f(*args, **kwargs, session=session):
                yield data
    return sub

def with_current_session_kwargs(f):
    @wraps(f)
    async def sub(*args, **kwargs):
        return await f(*args, **kwargs, session=current_sqlalchemy_session.get())
    return sub


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from baseimplems.persistence.model_utils.model_utils_common import WithID
    from baseimplems.persistence.mixins import BaseMixins

    import anyio


    class Test(*BaseMixins, WithID):
        __tablename__ = 'test'


    async def main():
        async with (
            run_with_temporarily_persistent_mock_db_engine(),
            SqlalchemyBaseHandler() as db,
            db.session() as sess1,
            db.session() as sess2,
        ):
            print(f"[+] Valid DB obj: {db}, valid session: {current_sqlalchemy_session.get()}")

            async with run_within_session(sess1):
                print(f"[.] Running within session 1 {current_sqlalchemy_session.get()}")
                current_sqlalchemy_session.add(Test(id=3))
                await current_sqlalchemy_session.commit()

            async with run_within_session(sess2):
                print(f"[.] Running within session 2 {current_sqlalchemy_session.get()}")
                print(await Test.all())

            @with_auto_session_kwargs
            async def f(session):
                print(session)

            await f()

    anyio.run(main)
