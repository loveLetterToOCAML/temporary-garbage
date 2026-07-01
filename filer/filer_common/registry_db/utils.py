from functools import wraps

types = {}

# python types singleton to avoid redefining same parametrized types and try avoiding complex errors
def register_type(typename, depends_on):
    def sub(f):
        @wraps(f)
        def called_f(*args, **argv):
            dependencies = (typename, *depends_on(*args, **argv),)

            if dependencies in types:  # ignore created type as already existing, to avoid incoherence and create kind of type singleton
                return types[dependencies]

            # otherwise register the freshly created type
            types[dependencies] = f(*args, **argv)
            return types[dependencies]

        return called_f

    return sub
