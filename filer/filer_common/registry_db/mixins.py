from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.ext.hybrid import HybridExtensionType
from sqlalchemy.orm import RelationshipProperty
from sqlalchemy.util import classproperty
from sqlalchemy import inspect

from functools import reduce


class MergeMapperArgsMeta(DeclarativeMeta):

    def __new__(mcls, name, bases, dict):
        def merge(cur_dict, value):
            cur_dict.update(getattr(value, '__mapper_args__', {}))
            return cur_dict
        mapper_args = reduce(merge, bases, {})
        mapper_args.update(dict.get('__mapper_args__', {}))
        return DeclarativeMeta.__new__(mcls, name, bases,
                                       {**dict, '__mapper_args__': mapper_args} if mapper_args else dict)


class AutoTableCreationMeta(MergeMapperArgsMeta):
    # __init__ is ok in this case since it's only an external action to perform
    def __init__(cls, name, bases, dict):
        MergeMapperArgsMeta.__init__(cls, name, bases, dict)

        with get_engine() as engine:
            cls.metadata.create_all(engine)


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


def commit_and_rollback_if_exception(session):
    try:
        session.commit()
    except Exception as e:
        session.rollback()
        raise e

class SessionMixin:
    __abstract__ = True

    _session = None

    @classproperty
    def session(cls):
        if cls._session is None:
            cls._session = current_session()
        return cls._session

    @classproperty
    def query(cls):
        return cls.session.query(cls)

    @classmethod
    def query_for(cls, *args, **kwargs):
        return cls.session.query(cls).filter(*args).filter_by(**kwargs)

    def save(self, commit=True):
        self.session.add(self)
        if commit:
            commit_and_rollback_if_exception(self.session)
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
    def create(cls, commit=True, **argv):
        return cls().fill(**argv).save(commit=commit)

    def update(self, commit=True, **argv):
        return self.fill(**argv).save(commit=commit)

    def delete(self, commit=True):
        self.session.delete(self)
        if commit:
            commit_and_rollback_if_exception(self.session)

    @classmethod
    def delete_many(cls, *ids, commit=True):
        for pk in ids:
            obj = cls.find(pk)
            if obj:
                obj.delete(commit=commit)
        if not commit:  # otherwise changes are committed
            cls.session.flush()

    @classmethod
    def all(cls):
        return cls.query.all()

    @classmethod
    def first(cls):
        return cls.query.first()

    @classmethod
    def find(cls, id_):
        return cls.query.get(id_)


    @classmethod
    def get_for(cls, **attrs):
        return cls.query.filter_by(**attrs).one_or_none()

    @classmethod
    def get_create(cls, commit=True, **attrs):
        existing = cls.get_for(**attrs)
        return existing if existing else cls.create(commit=commit, **attrs)

    @classmethod
    def get_from_instance(cls, instance, commit=True):  # instance must be of cls type (must be introspectable)
        attrs = {x: getattr(instance, x, None) for x in instance.columns + instance.relations}
        attrs = {x: attrs[x] for x in attrs if attrs[x]}
        existing = cls.get_for(**attrs)
        return existing if existing else cls.create(commit=commit, **attrs)

    @classmethod
    def get_from_construct(cls, *args, **argv):
        # construct the object attributes in case of complex object
        instance = cls(commit=argv.get('commit', True), *args, **argv)
        # then retrieve it /create it from database
        return cls.get_from_instance(instance, commit=argv.get('commit', True))

    @classmethod
    def filter_by(cls, **attrs):
        return cls.query.filter_by(**attrs)

    @classmethod
    def filter(cls, condition):
        return cls.query.filter(condition)

    @classmethod
    def all_for(cls, **attrs):
        return cls.filter_by(**attrs).all()

    @classmethod
    def all_for_condition(cls, condition):
        return cls.filter(condition).all()


MergeMapperArgsMixin = declarative_base(metaclass=MergeMapperArgsMeta)
AutoTableCreationMixin = declarative_base(metaclass=AutoTableCreationMeta)


BaseMixins = (AutoTableCreationMixin, ReprMixin, RepositoryMixin)
