from enum import Enum


class FilerBackendIntentType(Enum):
    GetContent = 1
    GetContentSize = 2
    GetContentUlidForHash = 3
    GetContentHashForUlid = 4
    CheckContentForHashAndUlid = 5

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

    # errors or interaction below
    NotExistingContent = 40
    HashNotMatchingContent = 41
    AlreadyUploadedContent = 42
    NotEnoughSpace = 43
    AlreadyUploadedChunk = 44
    BadChunkUploaded = 45
    BadDeletionKey = 46
    DeletionKeyRequired = 47