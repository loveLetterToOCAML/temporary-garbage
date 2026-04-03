from filer.filer_type_registration import FilerSystem, FilerType
from basetypes.a_root import SerialType


FilerClientTypes = FilerSystem.register_serialization_node(SerialType.BaseTypes, FilerClientBaseTypes)
FilerClientGenerics = FilerSystem.register_serialization_node(SerialType.GenericsData, FilerClientGenericTypes)
FilerClientConfigs = FilerSystem.register_serialization_node(SerialType.Config, FilerClientConfig)
FilerClientContexts = FilerSystem.register_serialization_node(SerialType.Config, FilerClientContext)
FilerClient = FilerSystem.register_serialization_node(SerialType.Config, FilerClientConfig)
