from enum import Enum

from basetypes.implementation.basetypes_constraints import Ulid
from filer.base_intents import FilerIntentType


class FilerInteractionType(Enum):
    GetContentQuery = 1
    GetContentSizeQuery = 2
    GetContentUlidForHashQuery = 3
    GetContentHashForUlidQuery = 4
    CheckContentForHashAndUlidQuery = 5

    DeleteContent = 20
    UploadContent = 21
    UploadContentChunk = 22
    UploadReady = 23
    UploadProgress = 24

    CheckIntegrity = 30
    CheckBackend = 31

    #
    # QueryResult[ContentChunk, NotExistingContent]
    # QueryResult[int, NotExistingContent]
    # QueryResult[Ulid, NotExistingContent]
    # QueryResult[bytes, NotExistingContent]
    # QueryResult[bool, NotExistingContent | HashNotMatchingContent]
    # QueryWithEffectResult[bool, NotExistingContent | HashNotMatchingContent | DeletionFailed]
    UploadFinished = 31


GetContentQuery = register_interaction_query(FilerIntentType.GetContent, success=ContentChunk, error=NotExistingContent)
GetContentSizeQuery = register_interaction_query(FilerIntentType.GetContentSize, success=int, error=NotExistingContent)
GetContentUlidForHashQuery = register_interaction_query(FilerIntentType.GetContentUlidForHash, success=Ulid, error=NotExistingContent)
GetContentHashForUlidQuery = register_interaction_query(FilerIntentType.GetContentHashForUlid, success=bytes, error=NotExistingContent)
CheckContentForHashAndUlidQuery = register_interaction_query(FilerIntentType.CheckContentForHashAndUlid, success=bool, error=NotExistingContent)

DeleteContentQuery = register_interaction_query(FilerIntentType.DeleteContent, success=bool, error=NotExistingContent | HashNotMatchingContent | DeletionFailed)
register_interaction_query(FilerIntentType.GetContent, success=ContentChunk, error=NotExistingContent)

