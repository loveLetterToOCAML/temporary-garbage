from functools import wraps


sqlalchemy_types = {}

# python types singleton to avoid redefining same parametrized types and try avoiding complex errors
def register_sqlalchemy_type(typename, depends_on):
    def sub(f):
        @wraps(f)
        def called_f(*args, **argv):
            dependencies = (typename, *depends_on(*args, **argv),)
            if dependencies in sqlalchemy_types:  # ignore created type as already existing, to avoid incoherence and create kind of type singleton
                return sqlalchemy_types[dependencies]
            # otherwise register the freshly created type
            sqlalchemy_types[dependencies] = f(*args, **argv)
            return sqlalchemy_types[dependencies]
        return called_f
    return sub


def default_sqlalchemy_classname_keying(*args):
    return (hasattr(obj, '__tablename__') and obj.__tablename__ or (isinstance(obj, str) and obj) or obj.__name__ for obj in args)
