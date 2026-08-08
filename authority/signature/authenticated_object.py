from __future__ import annotations

from typing import Protocol, TypeVar, Generic, final
from dataclasses import dataclass


Identifier = TypeVar('Identifier')
SerializableObject = TypeVar('SerializableObject')


class InvalidSerializedObject(Exception):
    pass


class WithSerializedRepr(Protocol[SerializableObject]):
    object: SerializableObject
    serialized: bytes

    @final
    def __bytes__(self) -> bytes:
        return self.serialized

    @final
    def ensure_valid_exn(self):
        if not self.ensure_valid():
            raise InvalidSerializedObject(self)

    def ensure_valid(self) -> bool:
        ...

    @classmethod
    def serialize_exn(self, obj: SerializableObject) -> WithSerializedRepr[SerializableObject]:
        ...

    @classmethod
    def deserialize_exn(self, data: bytes) -> WithSerializedRepr[SerializableObject]:
        ...

    @classmethod
    def serialize_forget_source_exn(self, obj: SerializableObject) -> bytes:
        ...

    @classmethod
    def deserialize_forget_source_exn(self, data: bytes) -> SerializableObject:
        ...


IdentityType = TypeVar('IdentityType', bound=WithSerializedRepr)


class Identity(Protocol[Identifier, IdentityType]):
    is_verified: bool = False
    identity_proof: AuthenticatedObject[IdentityType, S] | None

    def get_unique_id(self) -> Identifier:
        ...

    @property
    @final
    def get_identity(self) -> IdentityType:
        return self.identity_proof.object

    @final
    def verify_exn(self):
        if not self.verify():
            raise InvalidSignature()

    @final
    def verify(self) -> bool:
        return self.identity_proof.is_signature_valid_exn()


VerifiableIdentityType = TypeVar('VerifiableIdentityType', bound=Identity)


class InvalidSignature(Exception):
    pass


class Signature(Protocol[VerifiableIdentityType]):

    def signer_identity(self) -> VerifiableIdentityType:
        ...

    def verify(self, signer: VerifiableIdentityType, payload: bytes) -> bool:
        ...

    @final
    def verify_exn(self, payload: bytes):
        signer_identity = self.signer_identity()
        if not signer_identity.is_verified:
            signer_identity.verify()
        if not self.verify(signer_identity, payload):
            raise InvalidSignature()


O = TypeVar("O", bound=WithSerializedRepr)
S = TypeVar("S", bound=Signature)

@dataclass(frozen=True, slots=True)
class AuthenticatedObject(Generic[O, S]):
    object: O
    signature: S

    def identity(self) -> Identity:
        return self.signature.signer_identity()

    def is_signature_valid_exn(self) -> bool:
        return self.signature.verify(self.object.serialized)

    def check_signature_exn(self):
        if not self.is_signature_valid_exn():
            raise InvalidSignature()


@dataclass(frozen=True, slots=True)
class AuthenticatedAllowedObject(AuthenticatedObject[O, S]):

    def is_allowed_exn(self) -> bool:
        return self.is_signature_valid_exn() and current_authorization_domain.check_authorization_for(self.object, self.signature)
