from basetypes.a_root import SerializationNode, Root, SerialType

from enum import Enum


class GenericDataType(Enum):
    DATA_STRUCTURE = 1
    ALIAS = 2


### BEGIN AUTO GENERATION
# Auto-generated from GenericType for auto-completion purpose
class GenericData(SerializationNode):
    DATA_STRUCTURE = ...
    ALIAS = ...
### END AUTO GENERATION


class DefaultDataStructureType(Enum):
    LIST = 1
    DICT = 2
    TUPLE = 3

    SIMPLE_TREE = 10
    TREE = 11


### BEGIN AUTO GENERATION
# Auto-generated from DefaultDataStructureType for auto-completion purpose
class DefaultDataStructure(SerializationNode):
    LIST = ...
    DICT = ...
    TUPLE = ...
    SIMPLE_TREE = ...
    TREE = ...
### END AUTO GENERATION


Generics: GenericData = Root.register_serialization_child(SerialType.GenericsData, GenericDataType)

GenericDataStructure = Generics.register_serialization_child(GenericDataType.DATA_STRUCTURE, DefaultDataStructureType)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(GenericDataType))
    print(generate_autocompletion_for_enum(DefaultDataStructureType))
