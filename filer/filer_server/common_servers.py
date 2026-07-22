from filer.filer_backend.backend_impl_fs import FilerBackendFsParameters
from filer.filer_backend.backend_proxy_constrained import GenericBackendParameters, ConstrainedBackendParameters
from filer.filer_server.server_base import EffectfulFilerServer, FilerServerParameters, FilerServerInitParameters
from basetypes.implementation.dataformat.compression import Lz4, LZ4CompressionParameters
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters
from filer.filer_common.registry_factory import InMemRegistryConfig, DbRegistryWithSqlitePathConfig
from filer.filer_common.registry_fs import FsRegistryConfig
from filer.filer_context import file_registry_path_for, sqlite_registry_path_for

from contextlib import asynccontextmanager


allowed_deletion_params = GenericBackendParameters(
    allowedRead=True,
    allowedWrite=True,
    allowedDeletion=True,
)

def default_constrained_inmem_backend():
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=FilerBackendInMemParameters(),
        # favor speed over compression
        compressDataAlgorithm=Lz4(
            compressionParameters=LZ4CompressionParameters(compressionLevel=1)
        ),
        compressThreshold=0.7
    )

def default_constrained_fs_backend(backend_type, backend_index):
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=FilerBackendFsParameters(basePath=file_backend_path_for(backend_type, backend_index)),
        compressDataAlgorithm=Lz4(
            compressionParameters=LZ4CompressionParameters(compressionLevel=3)
        ),
        compressThreshold=0.8
    )

def default_constrained_sql_backend(backend_type, backend_index):
    return ConstrainedBackendParameters(
        globalParameters=allowed_deletion_params,
        backendParameters=FilerBackendSqlParameters(),
        compressDataAlgorithm=Lz4(
            compressionParameters=LZ4CompressionParameters(compressionLevel=3)
        ),
        compressThreshold=0.8
    )

def default_in_memory_filer_server_parameters():
    # full in mem backend + registry, no persistence so no initParameters
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=None,
        backendParameters=default_constrained_inmem_backend(),
        registryParameters=InMemRegistryConfig()
    )

def in_memory_filer_server_parameters_with_hash_memory(name: str | None = None):
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=FilerServerInitParameters(),
        backendParameters=default_constrained_inmem_backend(),
        registryParameters=FsRegistryConfig(
            filename=file_registry_path_for('inmem', name or 'umember')
        )
    )

def default_fs_filer_server_parameters(name: str | None = None):
    # fs backend with sqlite registry
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=FilerServerInitParameters(),
        backendParameters=default_constrained_fs_backend(),
        registryParameters=DbRegistryWithSqlitePathConfig(
            dbFilename=sqlite_registry_path_for('fs', name or 'main')
        )
    )

def fs_filer_server_with_fs_registry_parameters(name: str | None = None):
    # fs backend with fs registry
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=FilerServerInitParameters(),
        backendParameters=default_constrained_fs_backend(),
        registryParameters=FsRegistryConfig(
            filename=file_registry_path_for('fs', name or 'main')
        )
    )

def default_sql_filer_server_parameters(name: str | None = None):
    # sqlite backend with sqlite registry
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=FilerServerInitParameters(),
        backendParameters=default_constrained_sql_backend(),
        registryParameters=DbRegistryWithSqlitePathConfig(
            dbFilename=sqlite_registry_path_for('sql', name or 'main')
        )
    )

def sql_filer_server_with_fs_registry_parameters(name: str | None = None):
    # sqlite backend with fs registry
    return FilerServerParameters(
        globalParameters=allowed_deletion_params,
        initParameters=FilerServerInitParameters(),
        backendParameters=default_constrained_sql_backend(),
        registryParameters=FsRegistryConfig(
            filename=file_registry_path_for('sql', name or 'main')
        )
    )

@asynccontextmanager
def in_memory_filer_server():
    server = EffectfulFilerServer(default_in_memory_filer_server_parameters())
    async with server as integrity_report:
        yield server
