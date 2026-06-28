from basetypes.implementation.basetypes_match import DefaultBaseType

from typing import Protocol


class FilerBackend(Protocol):

    def get_content_hash_for_ulid(self, ulid: DefaultBaseType.ULID):
        ...

    def get_content_ulid_for_hash(self, hash: bytes):
        ...

    def check_content_for_hash_and_ulid(self, hash: bytes, ulid: DefaultBaseType.ULID):
        ...

    def get_content_size(self, hash: bytes):
        ...

    def get_content_start(self, hash: bytes):
        ...

    def upload_content_start(self, expected_hash: bytes, wanted_size: int, expected_bytes_per_second: int):
        ...

    def upload_content_chunk(self, offset: int, data: bytes):
        ...

    def delete_content(self, hash: bytes):
        ...

    def confirm_delete_content(self, hash: bytes, key: bytes):
        ...
