from typing import Literal

from filer.filer_backend.utils_exn import PydanticException
from filer.base_exceptions import PydanticFilerException

from pydantic import BaseModel, ConfigDict, Field

from enum import Enum

from filer.filer_type_registration import FilerType


# This is already including in the set of Filer exceptions
#class BackendFailureType(Enum):
#    ExternalError = 1
#    NotExistingContent = 2
#    OutOfUploadConstraints = 3
#    OutOfDownloadConstraints = 4
#    LackOfStorageSize = 5


class ExternalFailureType(Enum):
    AccessError = 1    # network access error
    ProtocolError = 2  # inability to talk to the remote peer
    CommunicationMismatch = 3  # peer did not understand the order or current execution system the response
    ForbiddenError = 4  # permission denied on the remote system
    InternalError = 5   # any other OS error
    TriggeredSecurity = 6  # in case of DOS scenario identification

class ExternalFailure(PydanticException):
    externalFailureType: ExternalFailureType


class GenericFilerFailure(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    executionSystem: FilerType
    failure: ExternalFailure | PydanticFilerException
    humanMessage: str
    retryable: bool = False

    originalException: Exception | None = Field(
        default=None, exclude=True, repr=False
    )


class BackendFailure(GenericFilerFailure):
    executionSystem: Literal[FilerType.FilerBackend] = FilerType.FilerBackend

class RegistryFailure(GenericFilerFailure):
    executionSystem: Literal[FilerType.FilerRegistry] = FilerType.FilerRegistry
