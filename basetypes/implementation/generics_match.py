from __future__ import annotations

from typing import TypeVar, Generic, TypeVarTuple, List

from basetypes.a_root_params import RootSerial
from basetypes.ab_basetypes import BaseTypes
from pydantic import BaseModel

from basetypes.ac_generics import Generics, DefaultDataStructureType
from basetypes.implementation.basetypes_match import DefaultBaseType


class SerializedListInstance(BaseModel):
    instanceOf: BaseTypes.TYPE
    keyValues: list[bytes]


class SerializedDictType(BaseModel):
    keyType: BaseTypes.TYPE    # TODO: ensure there is no risk to set arbitrary type there: does serialization guarantee right hashable concept?
    valueType: BaseTypes.TYPE

class SerializedDictInstance(BaseModel):
    instanceOf: SerializedDictType
    keyValues: list[tuple[bytes, bytes]]  # like this we can do pattern matching on types from the template


class SerializedTupleType(BaseModel):
    productTypes: list[BaseTypes.TYPE]

class SerializedTupleInstance(BaseModel):
    instanceOf: SerializedTupleType
    productInstances: list[bytes]


# There is no need to implement union type, as during a real exchange data is reified and fixed into one type already


T = TypeVar('T')
U = TypeVar('U')
V = TypeVarTuple('V')


class SimpleTree(RootSerial, Generic[T]):
    Type: DefaultBaseType.TYPE = Generics.path_until(DefaultDataStructureType.SIMPLE_TREE)
    rootNode: T
    children: List[SimpleTree[T]] = []


class Tree(RootSerial, Generic[T, U]):
    Type: DefaultBaseType.TYPE = Generics.path_until(DefaultDataStructureType.TREE)
    rootNode: T
    nodeChildren: List[Tree[T, U]]
    leafChildren: List[U] = []


class Dict(RootSerial, Generic[T, U]):
    Type: DefaultBaseType.TYPE = Generics.path_until(DefaultDataStructureType.DICT)
    keyTypeHint: T
    valueTypeHint: U
    data: dict[T, U]


# left is serialization repr, right is related python default type for it
# one can note there is no type enforcement here within the type tree
# this comes within serialization and deserialization process, which will create (or parse data into)
# intermediary Serialized[*]Instance or other generics data type
class DefaultGenericType:
    LIST = list  # List[T]
    DICT = dict  # Dict[T, U]
    TUPLE = tuple  # Tuple[*V]

    SIMPLE_TREE = SimpleTree  # SimpleTree[T]
    TREE = Tree               # Tree[T, U]

supported_generic_types_attributes = [attr for attr in DefaultGenericType.__dict__ if attr[0] != '_']

_type_cache = {}

def match_against_generic(obj):
    t = type(obj)
    if t in _type_cache:
        return _type_cache[t]

    for attr in supported_generic_types_attributes:
        if isinstance(obj, getattr(DefaultGenericType, attr)):
            _type_cache[t] = getattr(DefaultGenericType, attr)
    return _type_cache[t]


if __name__ == '__main__':
    x = {}
    print(match_against_generic(x))
    print(match_against_generic(x))

    x = []
    print(match_against_generic(x))

    x = SimpleTree[int](rootNode=1)
    print(match_against_generic(x))

    x = SimpleTree[int](rootNode="1")
    print(match_against_generic(x))

    # suppose deserialized type to match into the right generic

