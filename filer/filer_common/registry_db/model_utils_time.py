# see https://github.com/alkanor/tmppubliccore.git

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import expression
from sqlalchemy.types import DateTime

from datetime import datetime, timezone


class utcnow(expression.FunctionElement):
    type = DateTime()
    inherit_cache = True

@compiles(utcnow, 'sqlite')
def utcnow__default(element, compiler, **kw):
    # sqlite uses UTC by default
    return "CURRENT_TIMESTAMP"

@compiles(utcnow, 'postgresql')
def pg_utcnow(element, compiler, **kw):
    return "TIMEZONE('utc', CURRENT_TIMESTAMP)"

@compiles(utcnow, 'mssql')
def ms_utcnow(element, compiler, **kw):
    return "GETUTCDATE()"


class CreatedAt:
    __abstract__ = True

    __created_at_name__ = 'created_at'
    __datetime_func__ = utcnow

    created_at: Mapped[datetime] = mapped_column(__created_at_name__,
                                                 DateTime(timezone=True),
                                                 server_default=utcnow())


class CreatedModifiedAt:
    __abstract__ = True

    __created_at_name__ = 'created_at'
    __updated_at_name__ = 'updated_at'
    __datetime_func__ = utcnow

    created_at: Mapped[datetime] = mapped_column(__created_at_name__,
                                                 DateTime(timezone=True),
                                                 server_default=utcnow())
    updated_at: Mapped[datetime] = mapped_column(__updated_at_name__,
                                                 DateTime(timezone=True),
                                                 server_default=utcnow(),
                                                 onupdate=lambda: datetime.now(timezone.utc))
