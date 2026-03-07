from pydantic import BaseModel, create_model

from typing import Type, Dict
from enum import Enum


class AlreadyExistingField(Exception):
    def __init__(self, field_name):
        super().__init__(f"Field {field_name} already exists")


"""
Create configuration pydantic object model from provided enum dict and additional attributes models
"""
def default_config_for_enum(
    children: Dict[Enum, Type[BaseModel]],
    additional_model: Type[BaseModel],
    model_name: str | None = None,
) -> Type[BaseModel]:

    fields = {}
    for member in children:
        fields[member.value] = children[member]

    for name, field in additional_model.model_fields.items():
        if name in fields:
            raise AlreadyExistingField(name)
        fields[name] = (field.annotation, field.default)

    return create_model(model_name or additional_model.__name__, **fields)
