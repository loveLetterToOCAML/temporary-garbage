from basetypes.implementation.basetypes_match import DefaultBaseType

import contextvars


BaseTypesModel = contextvars.ContextVar('base_types', default=DefaultBaseType)
