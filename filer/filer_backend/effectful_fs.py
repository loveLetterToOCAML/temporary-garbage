from pydantic import BaseModel

from typing import Literal
from pathlib import Path
from enum import Enum


class SideEffectType(Enum):
    INTERNAL_STATE = 1
    INTERNAL_FLOW_REDIRECT = 2
    TIME = 3
    OTHER_TASK = 4
    OTHER_LOCAL_EXECUTION_SYSTEM = 5
    OTHER_LOCAL_PROCESS = 6
    LOCAL_FS = 7
    LOCAL_OS = 8
    LOCAL_PERSISTENCE_OTHER = 9
    NETWORK = 10
    EXTERNAL_PERSISTENCE = 11
    EXTERNAL_QUERY = 12
    REMOTE_CONTEXT = 13

class SideEffect(BaseModel):
    sideEffectType: SideEffectType

class ExceptionSideEffect(SideEffect):
    sideEffectType: Literal[SideEffectType.INTERNAL_FLOW_REDIRECT] = SideEffectType.INTERNAL_FLOW_REDIRECT
    serializedException: BaseModel


class PersistenceOperationType(Enum):
    CREATE = 1              # only metadata
    CREATE_AND_RESERVE = 2  # reserve space in addition
    CREATE_AND_FILL = 3     # fill with initial content
    CREATE_LINK = 4         # create some kind of link to other resource (any)

    READ_METADATA = 10      # only metadata
    READ_CONTENT = 11       # with content
    LIST = 12               # for directories mostly

    UPDATE_METADATA = 20    # only touching / updating attributes that are not main content
    UPDATE_CONTENT = 21     # updating main content
    APPEND_CONTENT = 22     # appending to main content, this can be file added in directory
    TRUNCATE_CONTENT = 23   # truncating main content

    MOVE = 30               # delete old + create & fill new
    DELETE = 31             # simple deletion
    DELETE_CHILDREN = 32    # deletion below current element (in case of directory mostly)


class FsMetadata(BaseModel):
    ...  # TODO: fill this

class FsOperation(BaseModel):
    operationType: PersistenceOperationType

class FsCreate(FsOperation):
    operationType: Literal[PersistenceOperationType.CREATE] = PersistenceOperationType.CREATE

class FsCreateReserve(FsOperation):
    operationType: Literal[PersistenceOperationType.CREATE_AND_RESERVE] = PersistenceOperationType.CREATE_AND_RESERVE
    reservedBytes: int

class FsCreateFill(FsOperation):
    operationType: Literal[PersistenceOperationType.CREATE_AND_FILL] = PersistenceOperationType.CREATE_AND_FILL
    filledBytes: int

class FsCreateLink(FsOperation):
    operationType: Literal[PersistenceOperationType.CREATE_LINK] = PersistenceOperationType.CREATE_LINK
    linkTo: BaseModel

class FsReadMetadata(FsOperation):
    operationType: Literal[PersistenceOperationType.READ_METADATA] = PersistenceOperationType.READ_METADATA
    metadataRead: list[FsMetadata] | FsMetadata

class FsReadContent(FsOperation):
    operationType: Literal[PersistenceOperationType.READ_CONTENT] = PersistenceOperationType.READ_CONTENT
    fromOffset: int
    expectedSizeToRead: int
    sizeRead: int

class FsList(FsOperation):
    operationType: Literal[PersistenceOperationType.LIST] = PersistenceOperationType.LIST

class FsUpdateMetadata(FsOperation):
    operationType: Literal[PersistenceOperationType.UPDATE_METADATA] = PersistenceOperationType.UPDATE_METADATA
    metadataUpdated: list[FsMetadata] | FsMetadata

class FsUpdateContent(FsOperation):
    operationType: Literal[PersistenceOperationType.UPDATE_CONTENT] = PersistenceOperationType.UPDATE_CONTENT
    fromOffset: int
    sizeUpdated: int

class FsAppendContent(FsOperation):
    operationType: Literal[PersistenceOperationType.APPEND_CONTENT] = PersistenceOperationType.APPEND_CONTENT
    appendedBytes: int
    sizeAfter: int

class FsTruncateContent(FsOperation):
    operationType: Literal[PersistenceOperationType.TRUNCATE_CONTENT] = PersistenceOperationType.TRUNCATE_CONTENT
    truncatedBytes: int
    sizeAfter: int

class FsMove(FsOperation):
    operationType: Literal[PersistenceOperationType.MOVE] = PersistenceOperationType.MOVE
    targetPath: str

class FsDelete(FsOperation):
    operationType: Literal[PersistenceOperationType.DELETE] = PersistenceOperationType.DELETE

class FsDeleteChildren(FsOperation):
    operationType: Literal[PersistenceOperationType.DELETE_CHILDREN] = PersistenceOperationType.DELETE_CHILDREN
    deletedChildren: str | list[str]


class FsSideEffect(SideEffect):
    sideEffectType: Literal[SideEffectType.LOCAL_FS] = SideEffectType.LOCAL_OS
    operation: FsOperation
    path: str


# TODO: check there if we want to hide path details depending of security options (dev / prod, etc...)
def fs_side_effect_for(operation: FsOperation, path: str | Path):
    return FsSideEffect(
        operation=operation,
        path=path if isinstance(path, str) else f"{path.absolute()}"
    )
