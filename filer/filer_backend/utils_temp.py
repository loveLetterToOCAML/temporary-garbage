from baseimplems.persistence.sqlalchemy_persist import wait_for_input_with_timeout

from contextlib import asynccontextmanager

from anyio import TemporaryDirectory


@asynccontextmanager
async def enclose_within_temporary_dir_interactive_mock(timeout: int = 0x100):
    async with TemporaryDirectory(suffix='.temp') as d:
        print(f"[+] Created temporary dir at {d}")
        yield d
        await wait_for_input_with_timeout(f"Will exit the named scope for {d} in {timeout} seconds (or input enter to exit)", timeout)


if __name__ == '__main__':
    import anyio

    async def main():
        async with (
            enclose_within_temporary_dir_interactive_mock() as d
        ):
            print('do whatever you want, scope will not close until timeout or user interaction')

    anyio.run(main)
