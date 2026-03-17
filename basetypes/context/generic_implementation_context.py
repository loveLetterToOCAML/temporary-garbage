from basetypes.implementation.generics_match import DefaultGenericType

import contextvars


# forced to separate from basetypes implementation context otherwise circular dependency later
GenericTypesModel = contextvars.ContextVar('generic_types', default=DefaultGenericType)
