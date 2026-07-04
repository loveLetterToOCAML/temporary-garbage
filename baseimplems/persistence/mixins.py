from baseimplems.persistence.sqlalchemy_database import current_sqlalchemy_session

from sqlalchemy.orm import RelationshipProperty, DeclarativeBase
from sqlalchemy.ext.hybrid import HybridExtensionType
from sqlalchemy import inspect, select


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
        attrs = self.__repr_attrs__ if self.__repr_attrs__ else self.columns + \
                                                                [k for k in self.relations if k[:2] != '__']
        values = []
        for key in attrs:
            if key in self.primary_keys:
                continue
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
    async def query(cls):
        return await current_sqlalchemy_session.execute(select(cls))

    @classmethod
    async def query_for(cls, *args, **kwargs):
        return await current_sqlalchemy_session.execute(select(cls)).filter(*args).filter_by(**kwargs)

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
    async def all(cls):
        return (await cls.query).all()

    @classmethod
    async def first(cls):
        return (await cls.query).first()

    @classmethod
    async def find(cls, id_):
        return (await cls.query).get(id_)


    @classmethod
    async def get_for(cls, **attrs):
        return (await cls.query.filter_by(**attrs)).one_or_none()

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
    async def filter_by(cls, **attrs):
        return await cls.query.filter_by(**attrs)

    @classmethod
    async def filter(cls, condition):
        return await cls.query.filter(condition)

    @classmethod
    async def all_for(cls, **attrs):
        return (await cls.filter_by(**attrs)).all()

    @classmethod
    async def all_for_condition(cls, condition):
        return (await cls.filter(condition)).all()


CommonMixins = (ReprMixin, RepositoryMixin, MergeMapperArgsMixin)

class MainSqlalchemyBase(DeclarativeBase):
    pass

BaseMixins = (MainSqlalchemyBase, ReprMixin, RepositoryMixin, MergeMapperArgsMixin)
