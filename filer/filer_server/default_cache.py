from filer.filer_server.common_servers import default_fs_filer_server_parameters, default_sql_filer_server_parameters, \
    default_in_memory_filer_server_parameters
from filer.filer_server.server_chain import FilerServerChainParameters
from filer.filer_server.server_factory import FilerServerFor


fscp = FilerServerChainParameters(
    fasterServerParameters=default_fs_filer_server_parameters(),
    slowerServerParameters=default_sql_filer_server_parameters()
)
fscp2 = FilerServerChainParameters(
    fasterServerParameters=default_in_memory_filer_server_parameters(),
    slowerServerParameters=fscp
)
cache_server = FilerServerFor(fscp2)
