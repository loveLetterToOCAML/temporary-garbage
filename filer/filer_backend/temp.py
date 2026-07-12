from filer.base_types import GetContentSizeIntent, GetContentUlidForHashIntent, GetContentHashForUlidIntent, \
    CheckContentForHashAndUlidIntent, GetContentIntent, UploadContentIntent, DeleteContentIntent, UploadChunkIntent, UploadProgressIntent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailureType, ExternalFailure
from filer.filer_backend.backend_proto import EffectfulBackend

FilerBackendIntent = GetContentSizeIntent | GetContentUlidForHashIntent | GetContentHashForUlidIntent | CheckContentForHashAndUlidIntent | \
    GetContentIntent | UploadContentIntent | DeleteContentIntent

FilerBackendResult = QueryResult[int, NotExistingContent] | \
                      QueryResult[DefaultBaseType.ULID, NotExistingContent] | \
                      QueryResult[bytes, NotExistingContent] | \
                      QueryResult[bool, NotExistingContent | HashNotMatchingContent] | \
                      QueryResult[GetContentTicket, NotExistingContent] | \
                      QueryResult[UploadContentTicket, AlreadyUploadedContent | NotEnoughSpaceRemaining | OutOfSpaceConstraints] | \
                      QueryResult[bool, NotExistingContent | PermanentContent]

