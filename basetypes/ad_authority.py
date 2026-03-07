from enum import Enum


class AuthorityType(Enum):
    Authentication = 1
    Identification = 2
    AccessControl = 3


class AuthenticationType(Enum):
    CertificateAuthentication = 1
    AuthenticationProof = 2
    AsymmetricalPubkey = 3
    Biometry = 4
    Multifactor = 5


class IdentificationType(Enum):
    Self = 1
    Other = 2
    Group = 3
    Impersonation = 4


class AccessControlType(Enum):
    ABAC = 1
    RBAC = 2
