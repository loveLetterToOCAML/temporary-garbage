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

    INT_INDEXED = 4
    STR_INDEXED = 5
    UUID_INDEXED = 6
    ULID_INDEXED = 7
    HASH_INDEXED = 8

    SIMPLE_TREE = 10
    TREE = 11

    PARTIAL_TREE_NODE = 12  # to create SIMPLE_TREE[PARTIAL_TREE_NODE[T]] or TREE[PARTIAL_TREE_NODE[T], U]
    PARTIAL_NODE_COMPLETION_INFO = 13


### BEGIN AUTO GENERATION
# Auto-generated from DefaultDataStructureType for auto-completion purpose
class DefaultDataStructure(SerializationNode):
    LIST = ...
    DICT = ...
    TUPLE = ...
    INT_INDEXED = ...
    STR_INDEXED = ...
    UUID_INDEXED = ...
    ULID_INDEXED = ...
    SIMPLE_TREE = ...
    TREE = ...
    PARTIAL_TREE_NODE = ...
### END AUTO GENERATION


Generics: GenericData = Root.register_serialization_child(SerialType.GenericsData, GenericDataType)

GenericDataStructure = Generics.register_serialization_child(GenericDataType.DATA_STRUCTURE, DefaultDataStructureType)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(GenericDataType))
    print(generate_autocompletion_for_enum(DefaultDataStructureType))
