from __future__ import annotations

from dataclasses import field, dataclass

from baseimplems.anyio_utils import NotInAsyncContextManager

from anyio import AsyncContextManagerMixin
from pydantic import BaseModel

from contextlib import asynccontextmanager, _AsyncGeneratorContextManager
from typing import Protocol, TypeVar, Generic, AsyncIterable, Any
from functools import wraps


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


T = TypeVar('T')
X = TypeVar('X')


#class SimpleListQueryRequest(BaseModel):
#    offset: int = 0
#    limit: int = 0x1000
#    includesDeleted: bool = False

#class SimpleListQueryResponse(BaseModel, Generic[T]):
#    results: list[T]
#    hasMore: bool = False


@dataclass(frozen=True, slots=True)
class SimpleListQueryRequest:
    offset: int = 0
    limit: int = 0x100
    includesDeleted: bool = False
    sizeSuperiorTo: int | None = None
    sizeInferiorTo: int | None = None


@dataclass(frozen=True, slots=True)
class SimpleListQueryResponse(Generic[T]):
    items: list[T]
    total: int
    hasMore: bool = False


class Listable(Protocol[T, X]):

    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[T]:
        ...

    async def list_items_of_type(self, item_type: type[X], request: SimpleListQueryRequest) -> SimpleListQueryResponse[X]:
        ...


# `Any` type in addition for listable, so that specific implementations can increase what can be listed
class Registry(Listable[MetadataType, HashType | UlidType | MetadataType | Any], Protocol[HashType, UlidType, MetadataType]):

    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        ...

    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        ...

    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:  # convention: bool is if hash exists
        ...

    async def size_for_hash(self, hash: HashType) -> int | None:
        ...

    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:  # bool : True is convention for deleted elements
        ...

    async def old_metadata_for_hash(self, hash: HashType) -> MetadataType | None:  # returns the metadata even if object is deleted
        ...

    async def new_item(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0) -> UlidType:
        ...

    async def delete_item(self, hash: HashType) -> bool | None:  # responsibility remains to the implementer to handle soft-delete
        ...


def guarded(func):
    @wraps(func)
    async def guard(self, *args, **kwargs):
        if not self._async_context_active:
            raise NotInAsyncContextManager(func.__name__, self.__class__.__name__)
        return await func(self, *args, **kwargs)
    return guard

class RegistryInContext(Registry[HashType, UlidType, MetadataType], AsyncContextManagerMixin):

    def __init__(self, internal_registry, upper_async_context_manager: _AsyncGeneratorContextManager | None = None):
        self._internal_registry = internal_registry
        self._async_context_active = False
        self._upper_async_context_manager = upper_async_context_manager

    @guarded
    async def hash_for_ulid(self, ulid: UlidType) -> HashType | None:
        return await self._internal_registry.hash_for_ulid(ulid)

    @guarded
    async def ulid_for_hash(self, hash: HashType) -> UlidType | None:
        return await self._internal_registry.ulid_for_hash(hash)

    @guarded
    async def check_hash_and_ulid(self, hash: HashType, ulid: UlidType) -> bool | None:
        return await self._internal_registry.check_hash_and_ulid(hash, ulid)

    @guarded
    async def size_for_hash(self, hash: HashType) -> int | None:
        return await self._internal_registry.size_for_hash(hash)

    @guarded
    async def metadata_for_hash(self, hash: HashType) -> MetadataType | bool | None:
        return await self._internal_registry.metadata_for_hash(hash)

    @guarded
    async def old_metadata_for_hash(self, hash: HashType) -> MetadataType | None:
        return await self._internal_registry.old_metadata_for_hash(hash)

    @guarded
    async def new_item(self, hash: HashType, item_metadata: MetadataType, size_of_data: int = 0) -> UlidType:
        return await self._internal_registry.new_item(hash, item_metadata, size_of_data)

    @guarded
    async def delete_item(self, hash: HashType) -> bool | None:
        return await self._internal_registry.delete_item(hash)

    @guarded
    async def list_items(self, request: SimpleListQueryRequest) -> SimpleListQueryResponse[MetadataType]:
        return await self._internal_registry.list_items(request)

    @guarded
    async def list_items_of_type(self, item_type: type[HashType | UlidType | MetadataType | Any], request: SimpleListQueryRequest) -> \
            SimpleListQueryResponse[HashType | UlidType | MetadataType | Any]:
        return await self._internal_registry.list_items_of_type(item_type, request)

    @asynccontextmanager
    async def _enclose_activity_boolean(self):
        try:
            self._async_context_active = True
            yield self
        finally:
            self._async_context_active = False

    @asynccontextmanager
    async def __asynccontextmanager__(self) -> AsyncIterable[RegistryInContext]:
        if self._upper_async_context_manager:
            async with (
                self._upper_async_context_manager(),
                self._enclose_activity_boolean() as r
            ):
                yield r
            return

        async with self._enclose_activity_boolean() as r:
            yield r
