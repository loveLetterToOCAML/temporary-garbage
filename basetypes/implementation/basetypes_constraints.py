from pydantic import BaseModel

from enum import Enum


class StringConstraintType(Enum):
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

    # TODO: check if we could not isolate these and complete this infinite list separately
    PRINTABLE_STRING = 26
    NUMERICAL_STRING = 27
    ALPHABETICAL_STRING = 28
    ALPHANUMERICAL_STRING = 29
    SLUG_STRING = 30  # with _ and - allowed
    HEXADECIMAL_STRING = 31
    BASE64_ENCODED_STRING = 32
    BASE32_ENCODED_STRING = 33
    ASCII85_ENCODED_STRING = 34
    WEBURL_SAFE_ENCODED_STRING = 35
    XML_ENCODED_STRING = 36


class BytesConstraintType(Enum):
    OPAQUE = 1
    TYPE = 2
    CHILD_TYPE = 3

    X509_CERTIFICATE = 5


class IntConstraintType(Enum):
    ATTRIBUTE = 1


class StringWithConstraint(BaseModel):
    constraint: StringConstraintType
    data: str

class BytesWithConstraint(BaseModel):
    constraint: BytesConstraintType
    data: bytes

class IntWithConstraint(BaseModel):
    constraint: IntConstraintType
    data: int


# TODO: add validation of input data for each type
# Warning: some types are contextual, like the TYPE one which requires knowing all valid nodes of the type tree

class Uuid(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.UUID

class Ulid(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.ULID

class Json(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.JSON_STRING

class Yaml(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.YAML_STRING

class Toml(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.TOML_STRING

class SemVer(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.SEMANTIC_VERSION

class SecretRef(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.SECRET_REFERENCE

class CronString(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.CRON_STRING

class Color(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.COLOR

class Url(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.URL

class MacAddress(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.MAC_ADDRESS

class Ipv4Address(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.IPV4_ADDRESS

class Ipv4Range(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.IPV4_RANGE

class Ipv6Address(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.IPV6_ADDRESS

class Ipv6Range(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.IPV6_RANGE

class NetworkService(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.NETWORK_SERVICE

class Domain(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.DOMAIN

class Fqdn(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.FQDN

class Email(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.EMAIL

class PhoneNumber(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.PHONE_NUMBER

class SocialIdentity(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.SOCIAL_IDENTITY

class SocialNumber(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.SOCIAL_NUMBER

class CountryName(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.COUNTRY_NAME

class CountryShort(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.COUNTRY_SHORT

class Address(StringWithConstraint):
    constraint: StringConstraintType = StringConstraintType.ADDRESS


class Opaque(BytesWithConstraint):
    constraint: BytesConstraintType = BytesConstraintType.OPAQUE

class Type(BytesWithConstraint):
    constraint: BytesConstraintType = BytesConstraintType.TYPE

class ChildType(BytesWithConstraint):
    constraint: BytesConstraintType = BytesConstraintType.CHILD_TYPE

class Certificate(BytesWithConstraint):
    constraint: BytesConstraintType = BytesConstraintType.X509_CERTIFICATE


class Attribute(IntWithConstraint):
    constraint: IntConstraintType = IntConstraintType.ATTRIBUTE
