# chatgpt powered piece of code to handle instance of a basemodel
# pip install "pydantic_extra_types[all]"

from __future__ import annotations

from pydantic_core import PydanticUndefined
from pydantic import BaseModel, IPvAnyAddress

from pydantic import (
    AnyUrl, HttpUrl,
    SecretStr, SecretBytes,
    Json
)

from datetime import datetime, date, time, timedelta, UTC
from typing import (
    Any, get_origin, get_args,
    Literal, Annotated, Type, Dict, Union
)
from types import UnionType
from decimal import Decimal
from pathlib import Path
from uuid import UUID
from enum import Enum
import ipaddress


def example_for_type(tp: Any) -> Any:
    origin = get_origin(tp)
    args = get_args(tp)

    if origin is Annotated:
        return example_for_type(args[0])

    if origin is UnionType or origin is Union:
        non_none = [a for a in args if a is not type(None)]
        return example_for_type(non_none[0])

    if origin is Literal:
        return args[0]

    if tp is int:
        return 1337
    if tp is float:
        return 1337.0
    if tp is str:
        return "s1337"
    if tp is bool:
        return True
    if tp is bytes:
        return b"b1337"

    if tp is datetime:
        return datetime.now(UTC)
    if tp is date:
        return date.today()
    if tp is time:
        return time(12, 0)
    if tp is timedelta:
        return timedelta(seconds=37, minutes=13)
    if tp is Decimal:
        return Decimal("1.0")
    if tp is UUID:
        return UUID(int=0)
    if tp is Path:
        return Path("example.txt")

    if tp is IPvAnyAddress:
        return ipaddress.IPv4Address("127.0.0.1")

    if isinstance(tp, type) and issubclass(tp, Enum):
        return list(tp)[0]

    if isinstance(tp, type) and issubclass(tp, BaseModel):
        return example_model_instance(tp)

    if origin in (list, set, frozenset):
        return origin([example_for_type(args[0])])
    if origin is tuple:
        return tuple(example_for_type(a) for a in args)
    if origin is dict:
        return {
            example_for_type(args[0]): example_for_type(args[1])
        }

    if tp is EmailStr:
        return "user@example.com"
    if tp in (AnyUrl, HttpUrl):
        return "https://example.com"
    if tp is SecretStr:
        return SecretStr("secret")
    if tp is SecretBytes:
        return SecretBytes(b"secret")
    if tp is Json:
        return '{"key": "value"}'

    if tp is Color:
        return Color("#ff0000")
    if tp is PhoneNumber:
        return "+33766554433"
    if tp is CountryAlpha2:
        return "FR"
    if tp is CountryAlpha3:
        return "FRA"
    if tp is CountryShortName:
        return 'France'
    if tp is MacAddress:
        return "00:00:5e:00:53:af"
    if tp is SemanticVersion:
        return '1.3.37'
    if tp is DomainStr:
        return 'example.com'
    if tp is CronStr:
        return '*/5 * * * *'
    if tp is ULID:
        return _ULID()
    if tp is S3Path:
        return 's3://example.com/dir/file'

    try:
        return tp()
    except Exception as _:
        return None


def example_model_instance(model_cls: Type[BaseModel]) -> BaseModel:
    values = {}

    for name, field in model_cls.model_fields.items():
        if field.default is not PydanticUndefined:
            values[name] = field.default
        elif field.default_factory is not None:
            values[name] = field.default_factory()
        else:
            values[name] = example_for_type(field.annotation)

    return model_cls(**values)


if __name__ == "__main__":
    class CustomEnum(Enum):
        BASE = 1
        THEN = 2

    class ChildModel(BaseModel):
        a: list[str]
        b: set[CustomEnum]
        c: tuple[CustomEnum, int, str, float]
        d: Dict[str, CustomEnum]
        #e: EmailStr
        f: HttpUrl
        g: SecretStr
        h: SecretBytes
        i: Json
        # j: Color
        # k: PhoneNumber
        # l: CountryAlpha2
        # m: CountryAlpha3
        # n: CountryShortName
        # o: MacAddress
        # p: SemanticVersion
        # q: DomainStr
        # r: CronStr
        # s: ULID
        # t: S3Path

    class Base(BaseModel):
        a: int | str
        b: str | int
        c: Literal['1338']
        d: int
        e: float
        f: str
        g: bool
        h: bytes
        i: datetime
        j: date
        k: time
        l: timedelta
        m: Decimal
        n: UUID
        o: Path
        p: IPvAnyAddress
        q: CustomEnum
        r: ChildModel

    example = example_for_type(Base)
    print(example)

    import toml
    toml_string = toml.dumps(example.model_dump())
    print(toml_string)
