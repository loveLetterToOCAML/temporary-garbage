from typing import Dict, List, TypeVar, Generic
from abc import ABC

from pydantic import BaseModel


class HashableWithBytesRepr(ABC):
    def __hash__(self) -> int:
        ...

    def __eq__(self, other) -> bool:
        ...

    def __bytes__(self) -> bytes:
        ...

class PydanticHashableWithBytesRepr(HashableWithBytesRepr, BaseModel):
    pass


ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType', bound=BaseModel)
HashType = TypeVar('HashType', bound=PydanticHashableWithBytesRepr | str | bytes)


class IntegrityReport(BaseModel, Generic[HashType, ExternalResourceLocatorType]):
    unexpectedItems: Dict[ExternalResourceLocatorType, bool] = {}  # bool is convention for is_deleted
    contentNotMatchingHashes: Dict[HashType, bool] = {}            # bool is convention for is_deleted
    contentMatchingHashes: List[HashType] = []
    contentUnknownMatchingHashes: List[HashType] = []
