from baseimplems.contextvar_utils import ContextVarWrapper
from baseimplems.anyio_utils import run_within

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from anyio import NamedTemporaryFile
import aioconsole
import anyio

from contextlib import asynccontextmanager
from contextvars import ContextVar
import time
import os


sqlalchemy_sqlite_db_path = ContextVar[str]('db_path')
run_within_sqlite_db_path = run_within(str, sqlalchemy_sqlite_db_path)

sqlalchemy_db_engine = ContextVarWrapper[AsyncEngine]('sqlalchemy_db_engine')


run_with_mock_db_engine = run_within(
    create_async_engine,
    sqlalchemy_db_engine,
    default_bind_static_arguments={
        'url': 'sqlite+aiosqlite:///:memory:',
        'echo': True
    }
)

run_with_persistent_mock_db_engine = run_within(
    create_async_engine,
    sqlalchemy_db_engine,
    default_bind_static_arguments={
        'echo': True
    },
    default_bind_callable_arguments={
        'url': lambda: f"sqlite+aiosqlite:///{sqlalchemy_sqlite_db_path.get()}"
    }
)

run_with_default_sqlite_engine = run_within(
    create_async_engine,
    sqlalchemy_db_engine,
    default_bind_callable_arguments={
        'url': lambda: f"sqlite+aiosqlite:///{sqlalchemy_sqlite_db_path.get()}"
    }
)


async def attempt_unlink(fname, max_time: float = 3, sleep_time: float = 0.5):
    start = time.time()
    while time.time() - start < max_time:
        try:
            os.unlink(fname)
            return
        except Exception as e:
            print(f"[-] Failed to remove {fname}: {e}, will retry in {sleep_time}")
        await anyio.sleep(sleep_time)
    print(f"[-] Failed to remove {fname}, stopping deletion attempts, please check manually")

async def wait_for_input_with_timeout(prompt: str, timeout: float):
    with anyio.move_on_after(timeout) as scope:
        await aioconsole.ainput(prompt)
    if scope.cancelled_caught:
        print("\ntimed out, leaving anyway")

@asynccontextmanager
async def enclose_within_temporary_file_interactive_mock():
    try:
        async with NamedTemporaryFile(mode="wb+", suffix='.db', delete=False) as f:
            print(f"Temporary file name that will hold the SQLITE database: {f.name}")
            yield {'url': f"sqlite+aiosqlite:///{f.name}"}
            timeout = 0x100
            await wait_for_input_with_timeout(f"Will exit the named scope for {f.name} in {timeout} seconds (or input enter to exit)", timeout)
    finally:
        await attempt_unlink(f.name)


run_with_temporarily_persistent_mock_db_engine = run_within(
    create_async_engine,
    sqlalchemy_db_engine,
    default_bind_static_arguments={
        'echo': True,
    },
    upper_context_dependency=enclose_within_temporary_file_interactive_mock
)


if __name__ == '__main__':
    from sqlalchemy import text

    async def main():
        async with (
            run_within_sqlite_db_path('C:\\Users\\Alka\\AppData\\Local\\Temp\\tmps6jiis0q.db'),
            run_with_persistent_mock_db_engine() as engine
        ):
            print(engine)

        async with run_with_temporarily_persistent_mock_db_engine() as engine:
            try:
                async with engine.begin() as conn:
                    await conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))
                    await conn.execute(text("INSERT INTO users (name) VALUES ('alice'), ('bob')"))

                async with engine.connect() as conn:
                    result = await conn.execute(text("SELECT * FROM users"))
                    print(result.fetchall())
            finally:
                await engine.dispose()

    anyio.run(main)
