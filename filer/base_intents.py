from enum import Enum


class FilerIntentType(Enum):
    GetContent = 1
    GetContentSize = 2
    GetContentUlidForHash = 3
    GetContentHashForUlid = 4
    CheckContentForHashAndULID = 5

    DeleteContent = 20
    UploadContent = 21
    UploadContentChunk = 22
    UploadReady = 23
    UploadProgress = 24

    CheckIntegrity = 30
    CheckBackend = 31
