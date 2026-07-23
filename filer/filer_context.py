from baseimplems.contextvar_utils import ContextVarPropertyWrapper
from baseimplems.anyio_utils import run_within
from context.init import current_fs_base

from pydantic import BaseModel

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Literal
import os


default_filer_registry_name = 'registry-{type}.yaml'
default_filer_registry_name_with_id = 'registry-{type}-{id}.yaml'
default_filer_sql_registry_name_with_id = 'registry-{type}-{id}.sql'
default_filer_backend_name = 'backend-{name}'
default_filer_dirname = 'filer'


class FilerFsPersistenceConfig(BaseModel):
    filerServerName: str = default_filer_registry_name
    filerFsDirname: str = default_filer_dirname
    filerFsBase: str
    filerFsRegistryForInMemBackendPath: str
    filerFsRegistryForFsBackendPath: str
    filerFsRegistryForSqlBackendPath: str


@asynccontextmanager
async def default_filer_fs_persistence_config():
    path = os.path.join(current_fs_base.get(), default_filer_dirname)
    os.makedirs(path, exist_ok=True)
    yield {
        "filerFsBase": path,
        "filerFsRegistryForInMemBackendPath": os.path.join(path, default_filer_registry_name.format(type='inmem')),
        "filerFsRegistryForFsBackendPath": os.path.join(path, default_filer_registry_name.format(type='fs')),
        "filerFsRegistryForSqlBackendPath": os.path.join(path, default_filer_registry_name.format(type='sql'))
    }


current_filer_persistence_config = ContextVar[FilerFsPersistenceConfig]('filer_persistence_config')
run_with_default_filer_persistence_config = run_within(FilerFsPersistenceConfig, current_filer_persistence_config,
                                                       upper_context_dependency=default_filer_fs_persistence_config)

current_filer_fs_dirname = ContextVarPropertyWrapper[str](current_filer_persistence_config, 'filerFsDirname')
current_filer_fs_base = ContextVarPropertyWrapper[str](current_filer_persistence_config, 'filerFsBase')


def file_registry_path_for(filer_type: Literal['inmem'] | Literal['fs'] | Literal['sql'], registry_index: str | int):
    return os.path.join(current_filer_fs_base.get(), default_filer_registry_name_with_id.format(type=filer_type, id=registry_index))

def sqlite_registry_path_for(filer_type: Literal['inmem'] | Literal['fs'] | Literal['sql'], registry_index: str | int):
    return os.path.join(current_filer_fs_base.get(), default_filer_sql_registry_name_with_id.format(type=filer_type, id=registry_index))

def file_backend_basepath_for(backend_name: str | int):
    return os.path.join(current_filer_fs_base.get(), default_filer_backend_name.format(name=backend_name))


if __name__ == '__main__':
    import anyio

    async def main():
        async with run_with_default_filer_persistence_config():
            print(current_filer_persistence_config.get())
            print(current_filer_fs_dirname.get())
            print(current_filer_fs_base.get())
            print(file_registry_path_for('sql', 'firstone'))

    anyio.run(main)
