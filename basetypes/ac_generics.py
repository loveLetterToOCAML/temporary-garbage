from pydantic import BaseModel

from enum import Enum

from basetypes.ab_basetypes import BaseTypes


class GenericsDataType(Enum):
    DataStructure = 1

    AuthorityGenerics = 10
    InteractionGenerics = 11
    CommunicationGenerics = 12
    #SelfExecutionGenerics = 13  # no self execution generics: execution is the location of "physical" reified things


class GenericType(BaseModel):
    type: bytes

    @classmethod
    @property
    def name(cls):
        return cls.__name__


class BasePacket(BaseModel):
    pass


class DataCollection(BaseModel):
    pass


class Dict(BaseModel):
    keyType: GenericType
    valueType: GenericType

class DictInstance(BaseModel):
    instanceOf: Dict
    keyValues: list[tuple[BasePacket, BasePacket]]


class Tuple(BaseModel):
    productTypes: list[GenericType]

class TupleInstance(BaseModel):
    instanceOf: Tuple
    productInstances: list[BasePacket]



class SimpleTree(BaseModel):
    NodeType: BaseTypes.TYPE
    root: NodeType


class Tree(BaseModel):
    NodeType: TypeInTree
    LeafType: TypeInTree
    root: NodeType

