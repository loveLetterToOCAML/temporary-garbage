from baseimplems.persistence.sqlalchemy_database import current_sqlalchemy_session

from sqlalchemy.orm import RelationshipProperty, DeclarativeBase
from sqlalchemy.ext.hybrid import HybridExtensionType
from sqlalchemy.ext.asyncio import AsyncAttrs
from pydantic import create_model, ConfigDict
from sqlalchemy import inspect, select

from typing import Any


class classproperty:
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, obj, owner):
        return self.fget(owner)


class MergeMapperArgsMixin:
    def __init_subclass__(cls, **kw):
        merged = {}
        for base in cls.__mro__[1:]:
            merged.update(getattr(base, "__mapper_args__", {}))
        merged.update(cls.__dict__.get("__mapper_args__", {}))
        if merged:
            cls.__mapper_args__ = merged
        super().__init_subclass__(**kw)


# mostly inspired from https://github.com/absent1706/sqlalchemy-mixins/blob/master/sqlalchemy_mixins/inspection.py

class IntrospectionMixin:

    @classproperty
    def columns(cls):
        return inspect(cls).columns.keys()

    @classproperty
    def primary_keys_full(cls):
        mapper = cls.__mapper__
        return [
            mapper.get_property_by_column(column)
            for column in mapper.primary_key
        ]

    @classproperty
    def primary_keys(cls):
        return [pk.key for pk in cls.primary_keys_full]

    @classproperty
    def relations(cls):
        return [c.key for c in cls.__mapper__.attrs
                if isinstance(c, RelationshipProperty)]

    # TODO: test it
    @classproperty
    def hybrid_properties(cls):
        items = inspect(cls).all_orm_descriptors
        return [item.__name__ for item in items
                if item.extension_type == HybridExtensionType]


_TYPE_NAME_MAP = {
    int: "int",
    str: "str",
    float: "float",
    bool: "bool",
    bytes: "bytes",
}

def _python_type_name(python_type) -> str:
    name = _TYPE_NAME_MAP.get(python_type)
    if name:
        return name
    return python_type.__name__


class PydanticMixin:
    __abstract__ = True

    @classmethod
    def pydantic_from_sqlalchemy(cls, *, include: set[str] | None = None):
        mapper = inspect(cls)
        fields: dict[str, Any] = {}
        for column in mapper.columns:
            if include is not None and column.key not in include:
                continue
            python_type = column.type.python_type
            default = None if column.nullable else ...
            fields[column.key] = (python_type | None if column.nullable else python_type, default)
        return create_model(cls.__tablename__, __config__=ConfigDict(from_attributes=True), **fields)

    @classmethod
    def print_pydantic_model_source(
            cls,
            *,
            class_name: str | None = None,
            include: set[str] | None = None,
            exclude: set[str] | None = None,
            base_class: str = "BaseModel",
            from_attributes: bool = True,
    ) -> str:
        """
        Generate Python source for a Pydantic model mirroring the given
        SQLAlchemy model's columns. Prints/returns source text meant to be
        copy-pasted as a static class, not executed dynamically.
        """
        mapper = inspect(cls)
        exclude = exclude or set()

        lines: list[str] = []
        imports_needed: set[str] = set()

        resolved_name = class_name or f"{cls.__name__}Schema"

        lines.append(f"class {resolved_name}({base_class}):")
        if from_attributes:
            lines.append('    model_config = ConfigDict(from_attributes=True)')
            lines.append("")

        any_field = False
        for column in mapper.columns:
            key = column.key
            if include is not None and key not in include:
                continue
            if key in exclude:
                continue

            python_type = column.type.python_type
            type_name = _python_type_name(python_type)

            if python_type not in (int, str, float, bool, bytes):
                imports_needed.add(python_type.__module__)

            if column.nullable:
                annotation = f"{type_name} | None"
                default = " = None"
            else:
                annotation = type_name
                default = ""

            lines.append(f"    {key}: {annotation}{default}")
            any_field = True

        if not any_field:
            lines.append("    pass")

        source = "\n".join(lines)

        if imports_needed:
            hint = (
                    "# NOTE: verify/add imports for non-builtin types used above, e.g.:\n"
                    + "\n".join(f"#   from {mod} import ..." for mod in sorted(imports_needed))
                    + "\n\n"
            )
            source = hint + source

        return source

    @classmethod
    def print_pydantic_relationship_fields(cls) -> str:
        mapper = inspect(cls)
        rels = [
            attr.key for attr in mapper.attrs
            if isinstance(attr, RelationshipProperty)
        ]
        if not rels:
            return "# (no relationships found)"
        lines = ["# Relationships found on this model — decide manually how to expose each:"]
        for r in rels:
            lines.append(f"#   {r}: <NestedSchema> | int (id only) | omit entirely")
        return "\n".join(lines)


# mostly inspired from https://github.com/absent1706/sqlalchemy-mixins/blob/master/sqlalchemy_mixins/repr.py

class ReprMixin(IntrospectionMixin):
    __abstract__ = True

    __repr_attrs__ = []

    @classmethod
    def class_to_json(cls, max_nesting=-1, cur_nesting=0):
        return {
            'classname': cls.__name__,
            'tablename': cls.__tablename__,
            'attrs': {
                **{
                    k: getattr(cls, k).type for k in cls.columns
                },
                **{
                    k: '[...]' if cur_nesting >= max_nesting >= 0 else
                    (getattr(cls.__mapper__.attrs, k).argument.class_to_json(max_nesting, cur_nesting + 1)
                     if hasattr(getattr(cls.__mapper__.attrs, k).argument, 'class_to_json')
                     else getattr(cls.__mapper__.attrs, k).argument) for k in cls.relations
                }
            }
        }

    def self_to_json(self, max_nesting=-1, cur_nesting=0):
        return {
            'id': self._id_str,
            'classname': self.__class__.__name__,
            'tablename': self.__class__.__tablename__,
            'attrs': {
                **{
                    k: getattr(self, k) for k in self.columns
                },
                **{
                    k: '[...]' if cur_nesting >= max_nesting >= 0 else
                    (getattr(self, k).self_to_json(max_nesting, cur_nesting + 1)
                     if hasattr(getattr(self, k), 'self_to_json')
                     else getattr(self, k)) for k in self.relations if k[:2] != '__'
                }
            }
        }

    @property
    def _id_str(self):
        ids = inspect(self).identity
        if ids:
            return '-'.join([str(x) for x in ids]) if len(ids) > 1 \
                else str(ids[0])
        else:
            return 'None'

    @property
    def _repr_attrs_str(self):
        state = inspect(self)
        attrs = self.__repr_attrs__ if self.__repr_attrs__ else self.columns + \
                                                                [k for k in self.relations if k[:2] != '__']
        values = []
        for key in attrs:
            if key in self.primary_keys and state.identity is not None:
                continue
            if key in state.unloaded:
                value = '<unloaded>'
            else:
                value = getattr(self, key)
            wrap_in_quote = isinstance(value, str)
            if wrap_in_quote:
                value = f"'{value}'"
            values.append(f"{key}={value}")
        return ', '.join(values)

    def __repr__(self):
        repr_attrs = self._repr_attrs_str
        return '{'+f"{self.__class__.__tablename__} #{self._id_str}{' '+repr_attrs if repr_attrs else ''}"+'}'


async def commit_and_rollback_if_exception(session):
    try:
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise e

class SessionMixin:
    __abstract__ = True

    @classproperty
    def session(cls):
        return current_sqlalchemy_session.get()

    @classproperty
    def select(cls):
        return select(cls)

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.select.filter(*args).filter_by(**kwargs)

    @classmethod
    async def execute_query(cls, query):
        return await current_sqlalchemy_session.execute(query)

    async def save(self, commit=True):
        current_sqlalchemy_session.add(self)
        if commit:
            await commit_and_rollback_if_exception(current_sqlalchemy_session)
        return self


# mostly inspired from https://github.com/absent1706/sqlalchemy-mixins/blob/master/sqlalchemy_mixins/activerecord.py

class RepositoryMixin(SessionMixin):
    __abstract__ = True

    def fill(self, **attrs):
        for key in attrs:
            setattr(self, key, attrs[key])
        # trigger an object reconstruction, since we are not in the case of the SQLAlchemy mapper that triggers it
        if hasattr(self, 'init_on_load'):
            self.init_on_load()
        return self

    @classmethod
    async def create(cls, commit=True, **argv):
        data = cls()
        data.fill(**argv)
        await data.save(commit=commit)
        return data

    async def update(self, commit=True, **argv):
        data = self.fill(**argv)
        await data.save(commit=commit)

    async def delete(self, commit=True):
        await self.session.delete(self)
        if commit:
            await commit_and_rollback_if_exception(self.session)

    @classmethod
    async def delete_many(cls, *ids, commit=True):
        for pk in ids:
            obj = await cls.find(pk)
            if obj:
                await obj.delete(commit=commit)
        if not commit:  # otherwise changes are committed
            await cls.session.flush()

    @classmethod
    async def _execute(cls, stmt):
        return (await current_sqlalchemy_session.execute(stmt)).scalars()

    @classmethod
    async def all(cls):
        return (await cls._execute(cls.select)).all()

    @classmethod
    async def first(cls):
        return (await cls._execute(cls.select.limit(1))).first()

    @classmethod
    async def find(cls, id_):
        return await current_sqlalchemy_session.get().get(cls, id_)

    @classmethod
    async def get_for(cls, **attrs):
        a = (await cls._execute(cls.select.filter_by(**attrs).limit(2))).one_or_none()
        return a

    @classmethod
    async def get_create(cls, commit=True, **attrs):
        existing = await cls.get_for(**attrs)
        return existing if existing else await cls.create(commit=commit, **attrs)

    @classmethod
    async def get_from_instance(cls, instance, commit=True):  # instance must be of cls type (must be introspectable)
        attrs = {x: getattr(instance, x, None) for x in instance.columns + instance.relations}
        attrs = {x: attrs[x] for x in attrs if attrs[x]}
        existing = cls.get_for(**attrs)
        return existing if existing else await cls.create(commit=commit, **attrs)

    @classmethod
    async def get_from_construct(cls, *args, **argv):
        # construct the object attributes in case of complex object
        instance = cls(*args, **argv)
        # then retrieve it /create it from database
        return await cls.get_from_instance(instance, commit=argv.get('commit', True))

    @classmethod
    async def all_for(cls, **attrs):
        return (await cls._execute(cls.select.filter_by(**attrs))).all()

    @classmethod
    async def all_for_condition(cls, condition):
        return (await cls._execute(cls.select.filter(condition))).all()


CommonMixins = (ReprMixin, RepositoryMixin, MergeMapperArgsMixin)

class MainSqlalchemyBase(AsyncAttrs, DeclarativeBase):
    pass

BaseMixins = (MainSqlalchemyBase, PydanticMixin, ReprMixin, RepositoryMixin, MergeMapperArgsMixin)
