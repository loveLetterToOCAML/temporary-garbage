from pydantic import create_model, ConfigDict
from sqlalchemy import inspect

from typing import Any


def pydantic_from_sqlalchemy(model: type, *, name: str, include: set[str] | None = None):
    mapper = inspect(model)
    fields: dict[str, Any] = {}
    for column in mapper.columns:
        if include is not None and column.key not in include:
            continue
        python_type = column.type.python_type
        default = None if column.nullable else ...
        fields[column.key] = (python_type | None if column.nullable else python_type, default)
    return create_model(name, __config__=ConfigDict(from_attributes=True), **fields)
