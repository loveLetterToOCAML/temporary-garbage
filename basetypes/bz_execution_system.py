from basetypes.a_root import SerialType, SerializationNode, Root

from enum import Enum


class ExecutionSystemType(Enum):
    PersistentData = 1   # includes knowledge, filer, vault, cache, database, backup, structured logs
    Infrastructure = 2   # includes install, infra modelling, infra state storage and resource management
    History = 3
    Search = 4
    Social = 5

    Enumerate = 20     # maybe only in upper layer, don't know if there is a place for this in execution system category
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


### BEGIN AUTO GENERATION
# Auto-generated from ExecutionSystemType for auto-completion purpose
class ExecutionSystem(SerializationNode):
    PersistentData = ...
    Infrastructure = ...
    History = ...
    Search = ...
    Social = ...
    Enumerate = ...
    Instrument = ...
    ModelExternal = ...
    Measure = ...
    Other = ...
### END AUTO GENERATION


### BEGIN AUTO GENERATION
# Auto-generated from PersistentDataExecutionSystemType for auto-completion purpose
class PersistentDataExecutionSystem(SerializationNode):
    Filer = ...
    Knowledge = ...
    Vault = ...
    Cache = ...
    Database = ...
    Backup = ...
    LogSink = ...
### END AUTO GENERATION


Execution: ExecutionSystem = Root.register_serialization_child(SerialType.ExecutionSystem, ExecutionSystemType)
PersistentData: PersistentDataExecutionSystem = Execution.register_serialization_child(ExecutionSystemType.PersistentData, PersistentDataExecutionSystemType)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(ExecutionSystemType))
    print(generate_autocompletion_for_enum(PersistentDataExecutionSystemType))
