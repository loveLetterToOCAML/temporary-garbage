from basetypes.implementation.generics_match import DefaultGenericType

from typing import get_origin, get_args, Annotated, Union, Literal, Any
from types import UnionType


def example_for_type(tp: DefaultGenericType) -> Any:
    origin = get_origin(tp)
    args = get_args(tp)

    if origin is Annotated:
        return example_for_type(args[0])

    if origin is UnionType or origin is Union:
        non_none = [a for a in args if a is not type(None)]
        return example_for_type(non_none[0])

    if origin is Literal:
        return args[0]