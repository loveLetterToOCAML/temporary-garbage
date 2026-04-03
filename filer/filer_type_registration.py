from basetypes.a_root import Serial
from basetypes.ae_interaction import Interaction, InteractionType
from basetypes.bz_execution_system import PersistentDataExecutionSystemType, PersistentData
from basetypes.a_root_params import RootSerial

from enum import Enum


class FilerType(Enum):
    RecursiveRoot = 0  # this is from where we know eternal loop through the serialization type system

    FilerClient = 1
    FilerBackend = 2
    FilerRegistry = 3
    FilerProxy = 4
    FilerMaster = 5
    FilerAdmin = 6


FilerSubsystem = PersistentData.register_serialization_child(PersistentDataExecutionSystemType.Filer, FilerType)

FilerCommon = FilerSubsystem.register_serialization_child(FilerType.RecursiveRoot, RootSerial)
