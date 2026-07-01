from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import Integer, String
from ulid import ULID


class WithID:
    __abstract__ = True

    __named_id__ = 'id'

    id: Mapped[int] = mapped_column(__named_id__, Integer, primary_key=True)


class WithULID:
    __abstract__ = True

    __named_ulid__ = 'ulid'

    ulid: Mapped[int] = mapped_column(__named_ulid__, String(26),
                                      nullable=False, primary_key=True, default=lambda: f"{ULID()}")


class WithStringHash:
    __abstract__ = True

    __named_hash__ = 'hash'

    hash: Mapped[int] = mapped_column(__named_hash__, String(0x100), nullable=False, primary_key=True)


class SizeAttributes:
    __abstract__ = True

    __size_of_data_name__ = 'size_of'

    size: Mapped[int] = mapped_column(__size_of_data_name__, Integer)
