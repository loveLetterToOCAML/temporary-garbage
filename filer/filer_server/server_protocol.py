from pydantic import BaseModel

from basetypes.implementation.dataformat.hashed import HashContextProtocol

from typing import Protocol, final, Callable, TypeVar, AsyncIterator
from functools import wraps

import traceback
import sys

from filer.filer_backend.backend_protocol import EffectfulBackend

ExternalResourceLocatorType = TypeVar('ExternalResourceLocatorType')
BackendFailureType = TypeVar('BackendFailureType')
HashType = TypeVar('HashType', bound=HashContextProtocol)  # just for the compute_new context manager


class TicketFromRegistry(BaseModel):
    pass


class SignedTicketFromRegistry(BaseModel):
    ticket: TicketFromRegistry
    signature: bytes


class EffectfulFilerServer(EffectfulBackend[ExternalResourceLocatorType, ServerFailureType]):

    async def size_of_content_at_exn(self, locator: ExternalResourceLocatorType) -> int:
        """query and returns the number of bytes of a content located by locator"""
        ...
