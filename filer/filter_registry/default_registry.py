from anyio import create_task_group

import contextlib


@contextlib.asynccontextmanager
def default_filer_registry():
    async with (
        default_in_memory_backend() as b1,
        default_filesystem_backend() as b2,
        FilerRegistry(b1, b2) as fr,
    ):
        yield fr


if __name__ == '__main__':
    import anyio

    async def main_registry_server():
        async with (
            default_init_config_context_log(),
            default_filer_registry() as registry_server,
            registry_server
        ):
            pass

    async def main():
        with create_task_group() as tg:
            tg.start_soon(main_registry_server)
            tg.start_soon(default_filer_client)

    anyio.run(main)
