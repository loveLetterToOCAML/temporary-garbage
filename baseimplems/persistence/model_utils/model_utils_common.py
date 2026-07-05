from baseimplems.persistence.model_utils.naming_context import prefix_policy, suffix_policy
from baseimplems.persistence.mixins import RepositoryMixin

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String, LargeBinary, Boolean
from ulid import ULID

from typing import TypeVar


class WithID:
    __abstract__ = True

    __named_id__ = 'id'

    id: Mapped[int] = mapped_column(__named_id__, Integer, primary_key=True, autoincrement=True)

class WithULID:
    __abstract__ = True

    __named_ulid__ = 'ulid'

    ulid: Mapped[ULID] = mapped_column(__named_ulid__, String(26),
                                       nullable=False, unique=True, default=lambda: f"{ULID()}")

class WithStringHash:
    __abstract__ = True

    __named_hash__ = 'hash'

    hash: Mapped[str] = mapped_column(__named_hash__, String(0x200), nullable=False, unique=True)

class WithBytesHash:
    __abstract__ = True

    __named_hash__ = 'hash'

    hash: Mapped[bytes] = mapped_column(__named_hash__, LargeBinary(0x200), nullable=False, unique=True)

class WithULIDPrimaryKey:
    __abstract__ = True

    __named_ulid__ = 'ulid'

    ulid: Mapped[str] = mapped_column(__named_ulid__, String(26),
                                      nullable=False, primary_key=True, default=lambda: f"{ULID()}")

class WithStringHashPrimaryKey:
    __abstract__ = True

    __named_hash__ = 'hash'

    hash: Mapped[str] = mapped_column(__named_hash__, String(0x200), nullable=False, primary_key=True)

class WithBytesHashPrimaryKey:
    __abstract__ = True

    __named_hash__ = 'hash'

    hash: Mapped[bytes] = mapped_column(__named_hash__, LargeBinary(0x200), nullable=False, primary_key=True)


class WithSoftDelete:
    __abstract__ = True

    __named_is_deleted__ = 'is_deleted'

    is_deleted: Mapped[bool] = mapped_column(__named_is_deleted__, Boolean, default=False)


MAX_NAME_LENGTH = 255

class WithUniqueName(RepositoryMixin):
    __abstract__ = True

    __named_name__ = 'name'

    name: Mapped[str] = mapped_column(__named_name__, String(MAX_NAME_LENGTH), unique=True, nullable=False)

    @classmethod
    async def force_create(cls, name: str, commit: bool = True, force_index: bool = False, **attrs):
        index = 1
        modified_name = f"{prefix_policy(cls, index, name, force_index)}{suffix_policy(cls, index, name, force_index)}"
        while cls.find(modified_name):
            index += 1
            modified_name = f"{prefix_policy(cls, index, name, True)}{suffix_policy(cls, index, name, True)}"
        return await cls().fill(name=modified_name, **attrs).save(commit=commit)


class WithSizeAttributes:
    __abstract__ = True

    __size_of_data_name__ = 'size_of'

    size: Mapped[int] = mapped_column(__size_of_data_name__, Integer, default=0)


TWithID = TypeVar("TWithID", bound=WithID)
TWithULID = TypeVar("TWithULID", bound=WithULID)
TWithStringHash = TypeVar("TWithStringHash", bound=WithStringHash)
TWithBytesHash = TypeVar("TWithBytesHash", bound=WithBytesHash)
