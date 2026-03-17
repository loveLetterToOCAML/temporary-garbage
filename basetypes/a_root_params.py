# forced to split RootParams declaration from Root declaration to avoid circular dependency with BaseDataType
from basetypes.implementation.basetypes_match import DefaultBaseType
from basetypes.a_root import Root

from pydantic import BaseModel

from enum import Enum


class RootSerial(BaseModel):
    Type: DefaultBaseType.TYPE

# better explicit than implicit / dynamic: explicitly declare the attributes as params, do it for every subclass
# 0 -> 0x10: precedence happens at child level
# 0x10 -> 0xffff?: precedence happens at parent level (so as Type is below 0x10 Type is defined here for all subtypes)
class SerialParams(Enum):
    Type = 1

#RootParams = register_serialization_params_context(Root, SerialParams)
