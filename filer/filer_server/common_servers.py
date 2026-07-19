from filer.filer_backend.backend_proxy_constrained import GenericBackendParameters, ConstrainedBackendParameters
from basetypes.implementation.dataformat.compression import Lz4, LZ4CompressionParameters
from filer.filer_server.server_base import EffectfulFilerServer, FilerServerParameters
from filer.filer_backend.backend_impl_inmem import FilerBackendInMemParameters

from contextlib import asynccontextmanager


allowed_deletion_params = GenericBackendParameters(
    allowedRead=True,
    allowedWrite=True,
    allowedDeletion=True,
)

@asynccontextmanager
def in_memory_filer_server():
    server = EffectfulFilerServer(
        FilerServerParameters(
            globalParameters=allowed_deletion_params,
            initParameters=None,
            backendParameters=ConstrainedBackendParameters(
                globalParameters=allowed_deletion_params,
                backendParameters=FilerBackendInMemParameters(),
                # favor speed over compression
                compressDataAlgorithm=Lz4(
                    compressionParameters=LZ4CompressionParameters(compressionLevel=1)
                ),
                compressThreshold=0.7
            )
        ),
    )
    async with server as integrity_report:
        yield server
