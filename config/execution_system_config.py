from config.root_config import RootConfigContext
from basetypes.a_root import SerialType

from enum import Enum


class ExecutionSystemType(Enum):
    PersistentData = 1   # includes knowledge, filer, vault, cache, database, backup
    Infrastructure = 2   # includes install, infra modelling, infra state storage and resource management
    History = 3
    Search = 4
    Social = 5

    Enumerate = 20
    Instrument = 21
    ModelExternal = 22
    Measure = 23

    Other = 0xff


class PersistentDataExecutionSystemType(Enum):
    Filer = 1
    Knowledge = 2
    Vault = 3
    Cache = 4
    Database = 5
    Backup = 6
    LogSink = 7


ExecutionSystemConfigContext = RootConfigContext.register_child(
    SerialType.ExecutionSystem,
    SubconfigEnum=ExecutionSystemType
)

PersistentDataConfigContext = ExecutionSystemConfigContext.register_child(
    SerialType.PersistentData,
    SubconfigEnum=PersistentDataExecutionSystemType
)
