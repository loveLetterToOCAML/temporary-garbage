from __future__ import annotations

from baseimplems.persistence.sqlalchemy_persist import attempt_unlink, wait_for_input_with_timeout
from filer.filer_common.registry_protocol import RegistryInContext
from filer.filer_common.registry_inmem import InMemRegistry

from anyio import AsyncContextManagerMixin, Lock, NamedTemporaryFile, open_file, create_task_group, Event, CancelScope, sleep
from pydantic import BaseModel

from typing import TypeVar, Literal, AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
import os.path
import yaml
import json


HashType = TypeVar('HashType', bound=str | bytes | BaseModel)
UlidType = TypeVar('UlidType')
MetadataType = TypeVar('MetadataType', bound=BaseModel)


class FsRegistryConfig(BaseModel):
    filename: Path | str | None = None
    extension: Literal['YAML'] | Literal['JSON'] = 'YAML'
    allowRegistryRewriteIfBadFormat: bool = False
    allowSubdirCreation: bool = False
    mock: bool = False
    autosaveDelaySeconds: float = 30.0


class FsRegistryInContext(RegistryInContext[HashType, UlidType, MetadataType], AsyncContextManagerMixin):

    def __init__(self, params: FsRegistryConfig, *, hash_type: type[HashType], ulid_type: type[UlidType], metadata_type: type[MetadataType]):
        self._params = params
        self._hash_type = hash_type
        self._ulid_type = ulid_type
        self._metadata_type = metadata_type
        super().__init__(None, self._ensure_registry_file)  # will create the FsRegistry at context management time
        self._lock = None

    def construct_state_from_inmem_registry(self):
        return self._registry.dump_state()

    async def _load(self):
        async with (
            self._lock,
            await open_file(self._filename, 'r') as f
        ):
            content = await f.read()
            return json.loads(content) if self._params.extension == 'JSON' else yaml.safe_load(content)

    async def save_to(self, out_file: str | None = None):
        fname = out_file or self._filename
        async with (
            self._lock,
            await open_file(fname, 'w') as f
        ):
            data = self.construct_state_from_inmem_registry()
            await f.write(json.dumps(data) if self._params.extension == 'JSON' else yaml.safe_dump(data))

    async def _regularly_save(self, delay):
        with CancelScope() as self._autosave_loop:
            self._cancel_loop_ready.set()
            while True:
                await sleep(delay)
                await self.save_to()

    @asynccontextmanager
    async def _internal_load_and_save(self, filename) -> AsyncIterator[FsRegistryInContext]:
        self._filename = filename

        try:
            base = await self._load()
            initial_metadata = base.get('metadata', {})
            initial_ulids = base.get('ulid', {})
            initial_deleted = base.get('deleted', [])
            initial_sizes = base.get('sizes', {})
            initial_metadata = {bytes.fromhex(k) if self._hash_type == bytes else self._hash_type(k): self._metadata_type(**v)
                                for k, v in initial_metadata.items()}
            initial_ulids = {bytes.fromhex(k) if self._hash_type == bytes else self._hash_type(k): self._ulid_type(v)
                             for k, v in initial_ulids.items()}
            initial_sizes = {bytes.fromhex(k) if self._hash_type == bytes else self._hash_type(k): v
                             for k, v in initial_sizes.items()}
            initial_deleted = [bytes.fromhex(k) if self._hash_type == bytes else self._hash_type(k) for k in initial_deleted]
        except Exception as e:
            if not self._params.allowRegistryRewriteIfBadFormat:
                raise Exception(f"Unable to load content of type {self._params.extension}: {e}")
            initial_metadata = None
            initial_ulids = None
            initial_deleted = None
            initial_sizes = None

        reg = InMemRegistry[HashType, UlidType, MetadataType](
            initial_metadata=initial_metadata,
            initial_ulids=initial_ulids,
            initial_deleted=initial_deleted,
            initial_sizes=initial_sizes,
            hash_type=self._hash_type,
            ulid_type=self._ulid_type,
            metadata_type=self._metadata_type
        )
        async with create_task_group() as tg:
            tg.start_soon(self._regularly_save, self._params.autosaveDelaySeconds)
            try:
                yield reg
            finally:
                await self._cancel_loop_ready.wait()
                self._autosave_loop.cancel()
                await self.save_to()

    @asynccontextmanager
    async def _ensure_registry_file(self):
        self._lock = Lock()  # instantiating the object there allows free ensuring of the context manager being entered
        self._cancel_loop_ready = Event()
        state_file = self._params.filename
        if (not state_file or not os.path.isfile(state_file)) and self._params.mock:
            try:
                async with (
                    NamedTemporaryFile(mode="w+", suffix='.yml' if self._params.extension == 'YAML' else '.json', delete_on_close=False) as f,
                ):
                    await f.write('{}')
                    f.close()
                    async with self._internal_load_and_save(f.name) as self._registry:
                        yield self._registry
            finally:
                await attempt_unlink(f.name)
            return
        elif not state_file:
            raise Exception('No valid filename provided nor mock mode selected in FsRegistryInContext')

        if self._params.allowSubdirCreation and not os.path.isfile(state_file):
            os.makedirs(os.path.dirname(state_file), exist_ok=True)
        async with self._internal_load_and_save(state_file) as self._registry:
            yield self._registry


if __name__ == '__main__':
    from filer.filer_common.registry_protocol import SimpleListQueryRequest, RegistryInContext

    from ulid import ULID

    import anyio


    class UlidWrapper(ULID):

        def __init__(self, s: str | None = None):
            if s:
                super().__init__(ULID.from_str(s).bytes)
            else:
                super().__init__()


    class M(BaseModel):
        a: int = 1
        b: str = 'b'

    async def wait_user_input():
        timeout = 0x40
        await wait_for_input_with_timeout(f"Will exit the named scope in {timeout} seconds (or input enter to exit)", timeout)

    async def test():
        async with NamedTemporaryFile(mode="w+", suffix='.yml', delete_on_close=False) as f:
            async with FsRegistryInContext[bytes, UlidWrapper, M](params=FsRegistryConfig(mock=True), hash_type=bytes, ulid_type=UlidWrapper, metadata_type=M) as mock:
                print(await mock.new_item(b'x', M(a=123), 186))
                print(await mock.new_item(b'y', M(b='metadata'), 80870))
                print(await mock.new_item(b'z', M(b='metadataz'), 1337))
                print(await mock.new_item(b'z', M(b='metadataz')))
                print(await mock.delete_item(b'y'))
                print(await mock.new_item(b'y', M(b='metadata2'), 888))
                print(await mock.delete_item(b'z'))
                print(await mock.list_items(SimpleListQueryRequest(limit=1)))
                print(await mock.list_items_of_type(bytes, SimpleListQueryRequest()))
                print(await mock.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))

                await mock.save_to(f.name)

            async with FsRegistryInContext[bytes, UlidWrapper, M](params=FsRegistryConfig(filename=f.name), hash_type=bytes, ulid_type=UlidWrapper, metadata_type=M) as second_one:
                print(await second_one.list_items_of_type(bytes, SimpleListQueryRequest()))
                print(await second_one.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))
                print(await second_one.new_item(b'w', M(b='metadataz')))
                print(await second_one.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))

            async with FsRegistryInContext[bytes, UlidWrapper, M](params=FsRegistryConfig(filename=f.name), hash_type=bytes, ulid_type=UlidWrapper, metadata_type=M) as second_one:
                print(await second_one.list_items_of_type(bytes, SimpleListQueryRequest()))
                print(await second_one.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))
                print(await second_one.new_item(b'u', M(b='metadataz')))
                print(await second_one.list_items_of_type(UlidWrapper, SimpleListQueryRequest()))

            await wait_user_input()

    anyio.run(test)
