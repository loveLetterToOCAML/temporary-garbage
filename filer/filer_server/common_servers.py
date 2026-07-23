from filer.filer_backend.backend_proxy_constrained import GenericBackendParameters, ConstrainedBackendParameters
from filer.filer_context import file_registry_path_for, sqlite_registry_path_for, file_backend_basepath_for
from filer.filer_common.registry_factory import InMemRegistryParameters, DbRegistryWithSqlitePathParameters
from filer.filer_server.server_base import FilerServerParameters, FilerServerInitParameters
from basetypes.implementation.dataformat.compression import Lz4, LZ4CompressionParameters
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters
from filer.filer_backend.backend_factory import DbBackendInContextParameters
from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters
from filer.filer_common.registry_fs import FsRegistryParameters

from enum import Enum


class PersistenceType(Enum):
    IN_MEMORY = 1
    LOCAL_FS = 2
    LOCAL_DB = 3
    REMOTE_DB = 4
    REMOTE_S3 = 5
    REMOTE_GIT = 6
    REMOTE_NETWORK = 7


allowed_deletion_params = GenericBackendParameters(
    allowedRead=True,
    allowedWrite=True,
    allowedDeletion=True,
)


def FilerServerParamsFor(registry_persistence: PersistenceType, backend_persistence: PersistenceType, name: str | None = None,
                         global_parameters: GenericBackendParameters = GenericBackendParameters(),
                         init_parameters: FilerServerInitParameters | None = FilerServerInitParameters(),
                         compression_level: int = 3, compression_threshold: float = 0.8):
    match backend_persistence:
        case PersistenceType.IN_MEMORY:
            backend = default_constrained_inmem_backend()
            fs_type = 'inmem'
            name = name or 'umember'
        case PersistenceType.LOCAL_FS:
            backend = default_constrained_fs_backend(name or 'main')
            fs_type = 'fs'
            name = name or 'main'
        case PersistenceType.LOCAL_DB:
            backend = default_constrained_sql_backend()
            fs_type = 'sql'
            name = name or 'main'
        case _:
            raise NotImplementedError

    match registry_persistence:
        case PersistenceType.IN_MEMORY:
            registry = InMemRegistryParameters()
        case PersistenceType.LOCAL_FS:
            registry = FsRegistryParameters(
                filename=file_registry_path_for(fs_type, name)
            )
        case PersistenceType.LOCAL_DB:
            registry = DbRegistryWithSqlitePathParameters(
                dbFilename=sqlite_registry_path_for(fs_type, name)
            )
        case _:
            raise NotImplementedError

    return FilerServerParameters(
        globalParameters=global_parameters,
        initParameters=init_parameters,
        backendParameters=ConstrainedBackendParameters(
            globalParameters=global_parameters,
            backendParameters=backend,
            # favor speed over compression
            compressDataAlgorithm=Lz4(compressionParameters=LZ4CompressionParameters(compressionLevel=compression_level)),
            compressThreshold=compression_threshold
        ),
        registryParameters=registry
    )


def default_constrained_inmem_backend():
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=FilerBackendInMemParameters(),
        # favor speed over compression
        compressDataAlgorithm=Lz4(compressionParameters=LZ4CompressionParameters(compressionLevel=1)),
        compressThreshold=0.7
    )

def default_constrained_fs_backend(backend_name: str):
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=FilerBackendFsParameters(basePath=file_backend_basepath_for(backend_name)),
        compressDataAlgorithm=Lz4(compressionParameters=LZ4CompressionParameters(compressionLevel=3)),
        compressThreshold=0.8
    )

def default_constrained_sql_backend():
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=DbBackendInContextParameters(),
        compressDataAlgorithm=Lz4(compressionParameters=LZ4CompressionParameters(compressionLevel=3)),
        compressThreshold=0.8
    )


def default_in_memory_filer_server_parameters():
    # full in mem backend + registry, no persistence so no initParameters
    return FilerServerParamsFor(
        PersistenceType.IN_MEMORY, PersistenceType.IN_MEMORY,
        init_parameters=None, compression_level=1, compression_threshold=0.7
    )

def in_memory_filer_server_parameters_with_hash_memory(name: str | None = None):
    return FilerServerParamsFor(
        PersistenceType.LOCAL_FS, PersistenceType.IN_MEMORY,
        name=name
    )

def default_fs_filer_server_parameters(name: str | None = None):
    # fs backend with sqlite registry
    return FilerServerParamsFor(
        PersistenceType.LOCAL_DB, PersistenceType.LOCAL_FS,
        name=name
    )

def fs_filer_server_with_fs_registry_parameters(name: str | None = None):
    # fs backend with fs registry
    return FilerServerParamsFor(
        PersistenceType.LOCAL_FS, PersistenceType.LOCAL_FS,
        name=name
    )

def default_sql_filer_server_parameters(name: str | None = None):
    # sqlite backend with sqlite registry
    return FilerServerParamsFor(
        PersistenceType.LOCAL_DB, PersistenceType.LOCAL_DB,
        name=name
    )

def sql_filer_server_with_fs_registry_parameters(name: str | None = None):
    # sqlite backend with fs registry
    return FilerServerParamsFor(
        PersistenceType.LOCAL_FS, PersistenceType.LOCAL_DB,
        name=name
    )
