from pydantic import BeforeValidator
import pydantic

from typing import TypeVar, Type, Annotated
from enum import Enum


EnumNameSerializer = pydantic.PlainSerializer(
    lambda e: e.name,
    return_type='str',
    when_used='always'
)

# TODO: this is more related to serializable type enums meaning str->int (IntEnum) enums
# Maybe we should enforce this there ()
E = TypeVar("E", bound=Enum)

def validate_enum_value(enum_cls: Type[E]):
    def sub(v: str | int | E) -> E:
        return enum_cls[v] if isinstance(v, str) else enum_cls(v) if isinstance(v, int) else v
    return sub

def SerializableEnum(enum_cls: Type[E]) -> type[E]:
    return Annotated[
        enum_cls,
        BeforeValidator(validate_enum_value(enum_cls)),
        EnumNameSerializer,
    ]
