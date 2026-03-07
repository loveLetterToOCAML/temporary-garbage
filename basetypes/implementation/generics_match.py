from __future__ import annotations

from typing import TypeVar, Generic, TypeVarTuple, List, Dict, Tuple

from basetypes.ab_basetypes import BaseTypes
from pydantic import BaseModel


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


class SimpleTree(BaseModel, Generic[T]):
    rootNode: T
    children: List[SimpleTree[T]]


class Tree(BaseModel, Generic[T, U]):
    rootNode: T
    nodeChildren: List[Tree[T, U]]
    leafChildren: List[U]


# left is serialization repr, right is related python default type for it
# one can note there is no type enforcement here within the type tree
# this comes within serialization and deserialization process, which will create (or parse data into)
# intermediary Serialized[*]Instance or other generics data type
class GenericType:
    LIST = List[T]
    DICT = Dict[T, U]
    TUPLE = Tuple[*V]

    SIMPLE_TREE = SimpleTree[T]
    TREE = Tree[T, U]

supported_generic_types_attributes = [attr for attr in GenericType.__dict__ if attr[0] != '_']
