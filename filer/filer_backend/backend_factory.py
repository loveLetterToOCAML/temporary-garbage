from filer.filer_backend.backend_proxy_constrained import ConstrainedBackendParameters, EffectfulConstrainedFilerBackend
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters, EffectfulFilerInMemBackend
from filer.filer_backend.backend_impl_sql import EffectfulFilerSqlBackend, DbBackendInContextParameters
from filer.filer_backend.backend_impl_fs_opti import EffectfulFilerFsBackend, FilerBackendOptimizedFsParameters
from basetypes.implementation.dataformat.hashed import Hashed, MixedMd5Sha256
from filer.filer_backend.backend_remote import RemoteBackendInContextParameters
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters


KnownFilerBackendParameters = FilerBackendInMemParameters | FilerBackendFsParameters | FilerBackendOptimizedFsParameters | \
                              DbBackendInContextParameters | RemoteBackendInContextParameters | ConstrainedBackendParameters


def FilerBackendFor(backend_params: KnownFilerBackendParameters):
    match backend_params:
        case FilerBackendInMemParameters():
            return EffectfulFilerInMemBackend(backend_params)
        case FilerBackendFsParameters() | FilerBackendOptimizedFsParameters():
            return EffectfulFilerFsBackend(backend_params)
        case DbBackendInContextParameters():
            return EffectfulFilerSqlBackend()
        case ConstrainedBackendParameters():
            return EffectfulConstrainedFilerBackend(backend_params)
        case _:
            raise NotImplementedError


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from filer.filer_backend.utils_temp import enclose_within_temporary_dir_interactive_mock
    from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy
    from policy.log import run_with_log_policy, LogLevel

    import anyio


    async def main():
        async with (
            run_with_log_policy(logLevel=LogLevel.INFO),
            enclose_within_temporary_dir_interactive_mock() as main_dir,
            run_with_temporarily_persistent_mock_db_engine(echo=False),
            run_within_sqlalchemy() as _,
        ):
            f1 = FilerBackendFor(FilerBackendInMemParameters())
            f2 = FilerBackendFor(FilerBackendFsParameters(basePath=main_dir))
            f3 = FilerBackendFor(DbBackendInContextParameters())

            print(await f1.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'a'), 0, 10000))
            print(await f2.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'b'), 0, 10001))
            print(await f3.prepare_placeholder_for_hash(Hashed(hashAlgorithm=MixedMd5Sha256(), hash=b'c'), 0, 10002))

    anyio.run(main)
