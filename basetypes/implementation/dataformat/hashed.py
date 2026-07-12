from pydantic import BaseModel

from typing import Protocol, final, Iterator, Type, Callable, Any, Literal
from hashlib import sha256, sha512, md5
from contextlib import contextmanager
from enum import Enum


class HashAlgorithm(Enum):
    SHA256 = 1
    SHA512 = 2
    MIXED_MD5_SHA256 = 3
    MIXED_SHA256_SHA512 = 4
    HMAC = 10
    PBKDF = 20
    CUSTOM_COMBINED = 100


class HashAlgorithmInstance(BaseModel):
    type: HashAlgorithm
    hashParameters: Any | None = None

class SHA256(HashAlgorithmInstance):
    type: Literal[HashAlgorithm.SHA256] = HashAlgorithm.SHA256

class SHA512(HashAlgorithmInstance):
    type: Literal[HashAlgorithm.SHA512] = HashAlgorithm.SHA512

class MixedMd5Sha256(HashAlgorithmInstance):
    type: Literal[HashAlgorithm.MIXED_MD5_SHA256] = HashAlgorithm.MIXED_MD5_SHA256

class MixedSha256Sha512(HashAlgorithmInstance):
    type: Literal[HashAlgorithm.MIXED_SHA256_SHA512] = HashAlgorithm.MIXED_SHA256_SHA512


class HashProtocol(Protocol):

    def update(self, data: bytes):
        ...

    def digest(self) -> bytes:
        ...

    def is_same(self, value: bytes) -> bool:
        ...


class Hashed(BaseModel):  # due to BaseModel metaclass we cannot make the Hashed inherits from HashContextProtocol
    hashAlgorithm: HashAlgorithmInstance
    hash: bytes

    def __hash__(self):
        return hash(self.hash)

    def __eq__(self, other):
        return self.hash == other.hash and self.hashAlgorithm == other.hashAlgorithm

    @final
    @contextmanager
    def compute_new(self) -> Iterator[HashProtocol]:
        hash_state = hash_protocol_for_type(self.hashAlgorithm)
        yield hash_state


class SerializableHash(HashProtocol):

    def __init__(self, alg_type: HashAlgorithmInstance):
        self._alg_type = alg_type

    @property
    def hash_algorithm(self) -> HashAlgorithmInstance:
        return self._alg_type

    def to_hashed(self) -> Hashed:
        return Hashed(
            hashAlgorithm=self.hash_algorithm,
            hash=self.digest()
        )


# This is just to simply encapsulate any hash protocol into safe context manager for hash computation limited lifetime
class HashContextProtocol(Protocol):

    @final
    @contextmanager
    def compute_new(self) -> Iterator[SerializableHash]:
        hash_state = self.fresh_hash_state()
        yield hash_state

    def fresh_hash_state(self) -> SerializableHash:
        ...


class CommonHashProtocol(SerializableHash, HashContextProtocol):
    def __init__(self, alg_type: HashAlgorithmInstance, HashCls: Type | Callable):
        super().__init__(alg_type)
        self._base_cls = HashCls
        self._state = self._base_cls()

    def update(self, data: bytes):
        self._state.update(data)

    def digest(self) -> bytes:
        return self._state.digest()

    def is_same(self, value: bytes) -> bool:
        return self.digest() == value

    def fresh_hash_state(self) -> SerializableHash:
        return CommonHashProtocol(self.hash_algorithm, self._base_cls)


class MixedHashProtocol(SerializableHash, HashContextProtocol):
    def __init__(self, alg_type: HashAlgorithmInstance, *HashCls: Type | Callable):
        super().__init__(alg_type)
        self._base_classes = HashCls
        self._states = [cls() for cls in self._base_classes]
        self._alg_type = alg_type

    def update(self, data: bytes):
        for s in self._states:
            s.update(data)

    def digest(self) -> bytes:
        return b''.join(map(lambda x: x.digest(), self._states))

    def is_same(self, value: bytes) -> bool:
        return self.digest() == value

    def fresh_hash_state(self) -> SerializableHash:
        return MixedHashProtocol(self.hash_algorithm, *self._base_classes)


def hash_protocol_for_type(hash_instance: HashAlgorithmInstance) -> HashContextProtocol:
    match hash_instance:
        case SHA256():
            return CommonHashProtocol(hash_instance, sha256)
        case SHA512():
            return CommonHashProtocol(hash_instance, sha512)
        case MixedMd5Sha256():
            return MixedHashProtocol(hash_instance, md5, sha256)
        case MixedSha256Sha512():
            return MixedHashProtocol(hash_instance, sha256, sha512)
        case _:
            raise NotImplementedError


if __name__ == '__main__':

    with hash_protocol_for_type(SHA512()).compute_new() as h:
        h.update(b'x')
        print(h.digest())

    with hash_protocol_for_type(SHA256()).compute_new() as h:
        h.update(b'x')
        print(h.digest())

    with hash_protocol_for_type(MixedMd5Sha256()).compute_new() as h:
        h.update(b'x')
        print(h.digest())
        print(h.is_same(bytes.fromhex('9dd4e461268c8034f5c8564e155c67a62d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881')))

    T = {}
    T[h.to_hashed()] = 1
    T[h.to_hashed()] = 2
    T[Hashed(
        hashAlgorithm=SHA512(),
        hash=bytes.fromhex('9dd4e461268c8034f5c8564e155c67a62d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881')
    )] = 3
    T[Hashed(
        hashAlgorithm=MixedMd5Sha256(),
        hash=bytes.fromhex(
            '9dd4e461268c8034f5c8564e155c67a62d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881')
    )] = 4
    T[Hashed(
        hashAlgorithm=SHA256(),
        hash=bytes.fromhex(
            '9dd4e461268c8034f5c8564e155c67a62d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881')
    )] = 5
    print(T)

    U = Hashed(
        hashAlgorithm=MixedMd5Sha256(),
        hash=bytes.fromhex(
            '9dd4e461268c8034f5c8564e155c67a62d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881')
    )
    with U.compute_new() as h2:
        h2.update(b'x')
        print(h2.is_same(U.hash))
