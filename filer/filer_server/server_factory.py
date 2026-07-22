from filer.filer_server.server_choice import FilerServerMultipleParameters, EffectfulFilerServerMultibackend
from filer.filer_server.server_chain import FilerServerChainParameters, EffectfulFilerServerChain
from filer.filer_server.server_base import FilerServerParameters, EffectfulFilerServer

from pydantic import BaseModel


class RemoteFilerServerInContextParameters(BaseModel):
    pass


KnownServerParameters = FilerServerParameters | FilerServerChainParameters | FilerServerMultipleParameters | RemoteFilerServerInContextParameters


def FilerServerFor(server_params: KnownServerParameters):
    match server_params:
        case FilerServerParameters():
            return EffectfulFilerServer(server_params)
        case FilerServerChainParameters():
            return EffectfulFilerServerChain(server_params)
        case FilerServerMultipleParameters():
            return EffectfulFilerServerMultibackend(server_params)
        case _:
            raise NotImplementedError


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from filer.filer_backend.utils_temp import enclose_within_temporary_dir_interactive_mock
    from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy
    from basetypes.implementation.dataformat.hashed import Hashed, MixedMd5Sha256
    from policy.log import run_with_log_policy, LogLevel

    import anyio


    async def main():
        async with (
            run_with_log_policy(logLevel=LogLevel.INFO),
            enclose_within_temporary_dir_interactive_mock() as main_dir,
            run_with_temporarily_persistent_mock_db_engine(echo=False),
            run_within_sqlalchemy() as _,
        ):
            f1 = FilerServerFor(FilerServerParameters())
            f2 = FilerServerFor(FilerServerChainParameters(basePath=main_dir))
            f3 = FilerServerFor(DbBackendInContextParameters())

            print(await f1.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'a'), 0, 10000))
            print(await f2.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'b'), 0, 10001))
            print(await f3.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'c'), 0, 10002))

    anyio.run(main)
