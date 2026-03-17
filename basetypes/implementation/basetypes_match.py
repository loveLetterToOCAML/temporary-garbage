from __future__ import annotations

from basetypes.implementation.basetypes_constraints import Opaque, Type, Attribute, Uuid, Ulid, Yaml, Toml, SemVer, \
    Certificate, SecretRef, CronString, Color, Json, Url, ChildType, MacAddress, Ipv4Address, Ipv4Range, Ipv6Address, Ipv6Range, \
    NetworkService, Domain, Fqdn, Email, PhoneNumber, SocialIdentity, SocialNumber, CountryName, CountryShort, Address

from pydantic import BaseModel

from typing import Dict, Any, List
from decimal import Decimal
import datetime


class IntEnumObject(BaseModel):
    enumType: DefaultBaseType.TYPE
    value: int | str

class StrEnumObject(BaseModel):
    enumType: DefaultBaseType.TYPE
    value: str


# among the three following classes, only the last one can be set as a valid type after serialization happens
# but RawSparseObject is the first constructed object after deserialization. It is less costly to keep it, but
# can also be converted when needed to full proper SparseObject
# TODO: add special method in RootSerial to automatically resolve RawSparseObject to SparseObject when some attribute is accessed
class TypeLengthValue(BaseModel):
    type: int
    length: int
    value: bytes

class RawSparseObject(BaseModel):
    objectType: DefaultBaseType.TYPE
    # attributes semantics depends on type, but can be first deserialized into this list
    # and conversely, a base Packet with a payload only (or a payload alone) can be transformed into class + type + tlvs
    attributes: List[TypeLengthValue] = []

class SparseObject(BaseModel):
    objectType: DefaultBaseType.TYPE
    attributes: Dict[DefaultBaseType.ATTRIBUTE, Any]  # Any is in fact RootSerial but it would cause circular dep


# left is serialization repr, right is related python default type for it
class DefaultBaseType:
    NONE = None
    BOOL = bool
    INT = int
    FLOAT = float
    DECIMAL = Decimal
    STRING = str
    BYTES = bytes
    INT_ENUM = IntEnumObject
    STR_ENUM = StrEnumObject


    OPAQUE = Opaque
    TYPE = Type
    CHILD_TYPE = ChildType
    ATTRIBUTE = Attribute
    SPARSE_OBJECT = SparseObject


    DATETIME = datetime.datetime
    DATE = datetime.date
    TIME = datetime.time
    TIMEDELTA = datetime.timedelta

    UUID = Uuid
    ULID = Ulid

    JSON_STRING = Json
    YAML_STRING = Yaml
    TOML_STRING = Toml

    SEMANTIC_VERSION = SemVer

    X509_CERTIFICATE = Certificate
    URL = Url

    SECRET_REFERENCE = SecretRef
    CRON_STRING = CronString
    COLOR = Color


class DefaultExternalNetworkBaseType:
    MAC_ADDRESS = MacAddress
    IPV4_ADDRESS = Ipv4Address
    IPV4_RANGE = Ipv4Range
    IPV6_ADDRESS = Ipv6Address
    IPV6_RANGE = Ipv6Range
    NETWORK_SERVICE = NetworkService
    DOMAIN = Domain
    FQDN = Fqdn


class DefaultExternalSocialBaseType:
    EMAIL = Email
    PHONE_NUMBER = PhoneNumber
    SOCIAL_IDENTITY = SocialIdentity
    SOCIAL_NUMBER = SocialNumber
    COUNTRY_NAME = CountryName
    COUNTRY_ALPHA = CountryShort
    ADDRESS = Address
    COMPANY = str


class DefaultExternalDataformatBaseType:
    CONTAINED = 1  # like tar, zip
    COMPRESSED = 2
    ENCRYPTED = 3

    IMAGE = 10
    SOUND = 11
    VIDEO = 12

    OLE = 20
    PDF = 21
    MS_CONTAINER = 22


supported_base_types_attributes = [attr for attr in DefaultBaseType.__dict__ if attr[0] != '_']
