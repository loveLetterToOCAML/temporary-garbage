from typing import Protocol, TypeVar, Generic

from pydantic import BaseModel

"""
A registry basically implements:
 * unique metadata access and manipulation
 * unique primary keys, here we retain ULID for time and hash for content unicity (both types are abstract)
 * regular check of unicity metadata
 * initialization of all metadata from an initially empty list
 * management of resources through time and soft-delete (ability to know if a resource has been previously encountered); this is not enforced through the prototype, how could it be?
 * we chose to enforce async, as retrieval of any information could be made asynchronously
"""

HashType = TypeVar('HashType')
UlidType = TypeVar('UlidType')
MetadataType = TypeVar('MetadataType')


T = TypeVar('T', bound=BaseModel)
X = TypeVar('X')

class SimpleListQueryRequest(BaseModel):
    offset: int = 0
    limit: int = 0x1000

class SimpleListQueryResponse(BaseModel, Generic[T]):
    results: list[T]
    hasMore: bool = False

class Listable(Protocol[T, X]):

    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[T]:
        ...

    async def list_items_of_type(self, item_type: type[X], request: SimpleListQueryRequest) -> SimpleListQueryResponse[X]:
        ...


class Registry(Listable[MetadataType, HashType | UlidType | MetadataType], Protocol[HashType, UlidType, MetadataType]):

    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        ...

    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        ...

    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        ...

    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:  # bool : True is convention for deleted elements
        ...

    async def new_item(self, hash: HashType, item_metadata: MetadataType) -> UlidType:
        ...

    async def delete_item(self, hash: HashType) -> bool | None:  # responsibility remains to the implementer to handle soft-delete
        ...

    async def preload_metadata(self) -> int:  # `init` method, returns the number of metadata loaded
        ...
