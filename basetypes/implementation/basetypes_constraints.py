from pydantic import BaseModel

from enum import Enum


class StringConstraint(Enum):
    UUID = 1
    ULID = 2

    JSON_STRING = 3
    YAML_STRING = 4
    TOML_STRING = 5

    SEMANTIC_VERSION = 6

    SECRET_REFERENCE = 7
    CRON_STRING = 8
    COLOR = 9

    URL = 10
    MAC_ADDRESS = 11
    IPV4_ADDRESS = 12
    IPV4_RANGE = 13
    IPV6_ADDRESS = 14
    IPV6_RANGE = 15
    NETWORK_SERVICE = 16
    DOMAIN = 17
    FQDN = 18

    EMAIL = 19
    PHONE_NUMBER = 20
    SOCIAL_IDENTITY = 21
    SOCIAL_NUMBER = 22
    COUNTRY_NAME = 23
    COUNTRY_SHORT = 24
    ADDRESS = 25


class BytesConstraint(Enum):
    OPAQUE = 1
    TYPE = 2
    CHILD_TYPE = 3

    X509_CERTIFICATE = 5


class IntConstraint(Enum):
    ATTRIBUTE = 1


class StringWithConstraint(BaseModel):
    constraint: StringConstraint
    data: str

class BytesWithConstraint(BaseModel):
    constraint: BytesConstraint
    data: bytes

class IntWithConstraint(BaseModel):
    constraint: IntConstraint
    data: int


# TODO: add validation of input data for each type
# Warning: some types are contextual, like the TYPE one which requires knowing all valid nodes of the type tree

class Uuid(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.UUID

class Ulid(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.ULID

class Json(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.JSON_STRING

class Yaml(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.YAML_STRING

class Toml(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.TOML_STRING

class SemVer(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.SEMANTIC_VERSION

class SecretRef(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.SECRET_REFERENCE

class CronString(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.CRON_STRING

class Color(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.COLOR

class Url(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.URL

class MacAddress(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.MAC_ADDRESS

class Ipv4Address(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.IPV4_ADDRESS

class Ipv4Range(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.IPV4_RANGE

class Ipv6Address(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.IPV6_ADDRESS

class Ipv6Range(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.IPV6_RANGE

class NetworkService(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.NETWORK_SERVICE

class Domain(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.DOMAIN

class Fqdn(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.FQDN

class Email(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.EMAIL

class PhoneNumber(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.PHONE_NUMBER

class SocialIdentity(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.SOCIAL_IDENTITY

class SocialNumber(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.SOCIAL_NUMBER

class CountryName(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.COUNTRY_NAME

class CountryShort(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.COUNTRY_SHORT

class Address(StringWithConstraint):
    constraint: StringConstraint = StringConstraint.ADDRESS


class Opaque(BytesWithConstraint):
    constraint: BytesConstraint = BytesConstraint.OPAQUE

class Type(BytesWithConstraint):
    constraint: BytesConstraint = BytesConstraint.TYPE

class ChildType(BytesWithConstraint):
    constraint: BytesConstraint = BytesConstraint.CHILD_TYPE

class Certificate(BytesWithConstraint):
    constraint: BytesConstraint = BytesConstraint.X509_CERTIFICATE


class Attribute(IntWithConstraint):
    constraint: IntConstraint = IntConstraint.ATTRIBUTE
