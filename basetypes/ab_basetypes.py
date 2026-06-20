from typing import Any

from basetypes.implementation.basetypes_match import DefaultBaseType, supported_base_types_attributes
from basetypes.context.implementation_context import BaseTypesModel

from basetypes.a_root import SerialType, Root, SerializationNode

from enum import Enum


class BaseDataType(Enum):
    NONE = 1
    BOOL = 2
    INT = 3
    FLOAT = 4
    DECIMAL = 5
    STRING = 6
    BYTES = 7
    INT_ENUM = 8
    STR_ENUM = 9

    # from there and below all subtypes are kind of string with constraints on formatting or semantics
    OPAQUE = 100     # like bytes but may contain additional information about source / destination the opaque is dedicated to
    TYPE = 101       # this is simple uint8_t array to locate valid types in the full tree
    CHILD_TYPE = 102 # this marks a node and its children within the type tree, on the contrary to the TYPE which is specific node
    ATTRIBUTE = 103  # this is simple uint8_t array to locate the attribute type, with additional uint8_t / uint16_t ? for matching the attribute location
    SPARSE_OBJECT = 104  # this is type + attributes number + attributes for a given object

    DATETIME = 110
    DATE = 111
    TIME = 112
    TIMEDELTA = 113

    UUID = 120
    ULID = 121

    # purpose of current serialization process is to not serialize opaque as json, yaml or toml;
    # but specific externally related components with require them anyway so let's keep them in basic
    JSON_STRING = 130
    YAML_STRING = 131
    TOML_STRING = 132

    SEMANTIC_VERSION = 140

    X509_CERTIFICATE = 150
    URL = 151

    SECRET_REFERENCE = 160
    CRON_STRING = 161
    COLOR = 162


    OTHER_EXTERNAL_NETWORK = 240
    OTHER_EXTERNAL_SOCIAL = 241
    OTHER_EXTERNAL_DATAFORMAT = 242


class ExternalNetworkBaseType(Enum):
    MAC_ADDRESS = 1
    IPV4_ADDRESS = 2
    IPV4_RANGE = 3
    IPV6_ADDRESS = 4
    IPV6_RANGE = 5
    NETWORK_SERVICE = 6
    DOMAIN = 7
    FQDN = 8


class ExternalSocialBaseType(Enum):
    EMAIL = 1
    PHONE_NUMBER = 2
    SOCIAL_IDENTITY = 3
    SOCIAL_NUMBER = 4
    COUNTRY_NAME = 5
    COUNTRY_ALPHA = 6
    ADDRESS = 7
    COMPANY = 8


class DataformatBaseType(Enum):
    CONTAINED = 1  # like tar, zip
    COMPRESSED = 2
    ENCRYPTED = 3
    HASHED = 4
    SIGNED = 5

    IMAGE = 10
    SOUND = 11
    VIDEO = 12

    OLE = 20
    PDF = 21
    MS_CONTAINER = 22


### BEGIN AUTO GENERATION
# Auto-generated from BaseDataType for auto-completion purpose
class BaseData(SerializationNode):
    NONE = ...
    BOOL = ...
    INT = ...
    FLOAT = ...
    DECIMAL = ...
    STRING = ...
    BYTES = ...
    INT_ENUM = ...
    STR_ENUM = ...
    OPAQUE = ...
    TYPE = ...
    CHILD_TYPE = ...
    ATTRIBUTE = ...
    SPARSE_OBJECT = ...
    DATETIME = ...
    DATE = ...
    TIME = ...
    TIMEDELTA = ...
    UUID = ...
    ULID = ...
    JSON_STRING = ...
    YAML_STRING = ...
    TOML_STRING = ...
    SEMANTIC_VERSION = ...
    X509_CERTIFICATE = ...
    URL = ...
    SECRET_REFERENCE = ...
    CRON_STRING = ...
    COLOR = ...
    OTHER_EXTERNAL_NETWORK = ...
    OTHER_EXTERNAL_SOCIAL = ...
    OTHER_EXTERNAL_DATAFORMAT = ...
### END AUTO GENERATION

### BEGIN AUTO GENERATION
# Auto-generated from ExternalNetworkBaseType for auto-completion purpose
class ExternalNetworkBase(SerializationNode):
    MAC_ADDRESS = ...
    IPV4_ADDRESS = ...
    IPV4_RANGE = ...
    IPV6_ADDRESS = ...
    IPV6_RANGE = ...
    NETWORK_SERVICE = ...
    DOMAIN = ...
    FQDN = ...
### END AUTO GENERATION

### BEGIN AUTO GENERATION
# Auto-generated from ExternalSocialBaseType for auto-completion purpose
class ExternalSocialBase(SerializationNode):
    EMAIL = ...
    PHONE_NUMBER = ...
    SOCIAL_IDENTITY = ...
    SOCIAL_NUMBER = ...
    COUNTRY_NAME = ...
    COUNTRY_ALPHA = ...
    ADDRESS = ...
    COMPANY = ...
### END AUTO GENERATION

### BEGIN AUTO GENERATION
# Auto-generated from DataformatBaseType for auto-completion purpose
class DataformatBase(SerializationNode):
    CONTAINED = ...
    COMPRESSED = ...
    ENCRYPTED = ...
    HASHED = ...
    SIGNED = ...
    IMAGE = ...
    SOUND = ...
    VIDEO = ...
    OLE = ...
    PDF = ...
    MS_CONTAINER = ...
### END AUTO GENERATION


Base: BaseData = Root.register_serialization_child(SerialType.BaseTypes, BaseDataType)
ExternalNetworkBase: ExternalNetworkBase = Base.register_serialization_child(BaseDataType.OTHER_EXTERNAL_NETWORK, ExternalNetworkBaseType)
ExternalSocialBase: ExternalSocialBase = Base.register_serialization_child(BaseDataType.OTHER_EXTERNAL_SOCIAL, ExternalSocialBaseType)
ExternalDataFormatBase: DataformatBase = Base.register_serialization_child(BaseDataType.OTHER_EXTERNAL_DATAFORMAT, DataformatBaseType)


# don't know if this is best way to proceed to inject dependency of type implementation (do we really need this
# dependency injection at runtime? Guess this will be fixed and should not change a lot)
BaseTypes: DefaultBaseType = BaseTypesModel.get()

for attr_name in supported_base_types_attributes:
    Base.register_serialization_leaf(getattr(BaseDataType, attr_name), getattr(BaseTypes, attr_name))

# TODO: recursive descent on other basetypes ExternalNetworkBase, ExternalSocialBase or ExternalDataFormatBase
def attempt_serial_type(obj: Any):
    for t in supported_base_types_attributes:
        if type(obj) == getattr(DefaultBaseType, t):
            return getattr(BaseDataType, t)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(BaseDataType))
    print(generate_autocompletion_for_enum(ExternalNetworkBaseType))
    print(generate_autocompletion_for_enum(ExternalSocialBaseType))
    print(generate_autocompletion_for_enum(DataformatBaseType))

    print(attempt_serial_type('test'))
    print(Base.path_until(attempt_serial_type('test')))
