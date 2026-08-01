from filer.base_exceptions import FilerSerialException, NotExistingPlaceholder, AlreadyUploadedContent, AlreadyUploadingContent, MissingChunks
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from filer.filer_backend.backend_impl_fs_opti import none_if_exception
from filer.filer_backend.backend_protocol import EffectfulFilerBackendWithContextManagement, EffectfulFilerBackend, \
    EffectfulBackend
from filer.filer_backend.fixed_chunk_constraint import ensure_compatible_chunk_exn
from basetypes.implementation.dataformat.hashed import Hashed
from filer.filer_backend.utils_exn import SerialException

from botocore.exceptions import ClientError
from pydantic import Secret, BaseModel
from botocore.config import Config
from pydantic_core import Url
from anyio import open_file, AsyncContextManagerMixin
import aioboto3

from typing import AsyncIterator, AsyncIterable
from contextlib import asynccontextmanager


class FilerBackendS3Parameters(BaseModel):
    url: Url
    accessKey: str
    secretKey: Secret[str]
    bucketName: str
    signatureVersion: str = 's3v4'
    fixedChunkSize: int = 0x800000  # 8 Mb chunks by default


class S3ResourceLocator(BaseModel):
    key: str
    uploadId: str | None = None

    def __eq__(self, other):
        return self.key == other.key and self.uploadId == other.uploadId

    def __hash__(self):
        return (self.key, self.uploadId).__hash__()


class EffectfulFilerS3Backend(
    EffectfulBackend[S3ResourceLocator, Hashed],
    EffectfulFilerBackend[Hashed, S3ResourceLocator, BackendFailure],
    AsyncContextManagerMixin
):

    @property
    def _effectful_backend(self) -> EffectfulBackend[S3ResourceLocator, Hashed]:
        return self

    @none_if_exception
    def hash_from_resource_locator(self, locator: S3ResourceLocator) -> Hashed | None:
        return Hashed.str_deserialize_exn(locator.key) if locator.uploadId is None else None

    def resource_locator_from_hash(self, hash: Hashed) -> S3ResourceLocator:
        return S3ResourceLocator(
            key=hash.str_serialize()
        )

    def __init__(self, params: FilerBackendS3Parameters):
        self.session = aioboto3.Session()
        self._params = params

    @asynccontextmanager
    async def __asynccontextmanager__(self):
        self.session = aioboto3.Session()
        self._upload_ids_for = {}
        self._upload_status_for = {}
        self._expected_total_size_for = {}
        self._upload_parts = {}
        self._currently_uploading = {}
        async with (
            self.session.client(
            's3',
                endpoint_url=str(self._params.url),
                aws_access_key_id=self._params.accessKey,
                aws_secret_access_key=self._params.secretKey.get_secret_value(),
                config=Config(signature_version=self._params.signatureVersion)
            ) as self._s3_client
        ):
            yield  self

    def _bucket_key_for(self, locator: S3ResourceLocator):
        return {
            'Bucket': self._params.bucketName,
            'Key': locator.key
        }

    async def _ensure_non_existence(self, locator: S3ResourceLocator, placeholder_index: int | None = None):
        if placeholder_index is None:
            try:
                await self._s3_client.head_object(**self._bucket_key_for(locator))
            except:
                return
            raise FilerSerialException(
                AlreadyUploadedContent(
                    existingUlid=None,
                    hashAttempted=bytes(self.hash_from_resource_locator(locator))
                )
            )

    async def size_of_content_at_exn(self, locator: S3ResourceLocator) -> int:
        resp = await self._s3_client.head_object(**self._bucket_key_for(locator))
        return resp['ContentLength']

    async def prepare_placeholder_at_exn(self, locator: S3ResourceLocator, placeholder_index: int, total_size: int):
        await self._ensure_non_existence(locator)

        create_resp = await self._s3_client.create_multipart_upload(
            **self._bucket_key_for(locator)
        )
        if locator in self._currently_uploading:
            await self._s3_client.abort_multipart_upload(
                **self._bucket_key_for(locator),
                UploadId=create_resp['UploadId']
            )
            raise FilerSerialException(
                AlreadyUploadingContent(
                    hashUploading=bytes(self.hash_from_resource_locator(locator)),
                    placeholderIndex=placeholder_index
                )
            )
        self._currently_uploading[locator] = placeholder_index
        self._upload_ids_for[(locator, placeholder_index)] =  create_resp['UploadId']
        self._expected_total_size_for[(locator, placeholder_index)] = total_size
        self._upload_status_for[(locator, placeholder_index)] = {}  # cannot exceed size of 10000 as per AWS documentation / s3 spec
        return create_resp['UploadId']

    async def upload_chunk_at_exn(self, locator: S3ResourceLocator, placeholder_index: int, offset: int, data: bytes) -> int:
        if (locator, placeholder_index) not in self._upload_ids_for:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=bytes(self.hash_from_resource_locator(locator)),
                    placeholderIndex=placeholder_index
                )
            )

        size = len(data)
        # caller of upload is responsible for handling chunks of the right size according to chunkSize
        ensure_compatible_chunk_exn(offset, size, self._params.fixedChunkSize, self._expected_total_size_for[(locator, placeholder_index)])

        part_number = offset // self._params.fixedChunkSize
        resp = await self._s3_client.upload_part(
            **self._bucket_key_for(locator),
            PartNumber=part_number + 1,
            UploadId=self._upload_ids_for[(locator, placeholder_index)],
            Body=data,
        )
        self._upload_status_for[(locator, placeholder_index)][part_number] = {
            'ETag': resp['ETag'],
            'PartNumber': part_number + 1
        }
        return size

    async def upload_terminate_at_exn(self, locator: S3ResourceLocator, placeholder_index: int):
        if (locator, placeholder_index) not in self._upload_ids_for:
            raise FilerSerialException(
                NotExistingPlaceholder(
                    inputHash=bytes(self.hash_from_resource_locator(locator)),
                    placeholderIndex=placeholder_index
                )
            )

        bucket_kwargs = self._bucket_key_for(locator)
        upload_id = self._upload_ids_for[(locator, placeholder_index)]
        upload_parts = self._upload_status_for[(locator, placeholder_index)]
        expected_total_size = self._expected_total_size_for[(locator, placeholder_index)]
        expected_number_of_parts = expected_total_size // self._params.fixedChunkSize + \
                                   (1 if expected_total_size % self._params.fixedChunkSize else 0)

        del self._upload_status_for[(locator, placeholder_index)]
        del self._upload_ids_for[(locator, placeholder_index)]
        del self._expected_total_size_for[(locator, placeholder_index)]
        del self._currently_uploading[locator]

        if len(upload_parts) != expected_number_of_parts:
            await self._s3_client.abort_multipart_upload(
                **bucket_kwargs,
                UploadId=upload_id
            )
            raise FilerSerialException(
                MissingChunks(
                    inputHash=bytes(self.hash_from_resource_locator(locator)),
                    gotChunks=len(upload_parts),
                    expectedChunks=expected_number_of_parts,
                )
            )

        try:
            await self._s3_client.complete_multipart_upload(
                **bucket_kwargs,
                UploadId=upload_id,
                MultipartUpload={
                    'Parts': [upload_parts[i] for i in range(expected_number_of_parts)]
                },
            )
        except Exception as e:
            await self._s3_client.abort_multipart_upload(
                **bucket_kwargs,
                UploadId=upload_id
            )
            raise e

    async def download_chunk_from_exn(self, locator: S3ResourceLocator, offset: int, size: int) -> bytes:
        resp = await self._s3_client.get_object(
            **self._bucket_key_for(locator),
            Range=f"bytes={offset}-{offset + size}"
        )
        async with resp['Body'] as stream:
            return await stream.read()

    async def delete_resource_at_exn(self, locator: S3ResourceLocator, placeholder_index: int = -1):
        if placeholder_index >= 0:
            del self._currently_uploading[locator]
            del self._upload_status_for[(locator, placeholder_index)]
            del self._upload_ids_for[(locator, placeholder_index)]
            del self._expected_total_size_for[(locator, placeholder_index)]
        await self._s3_client.delete_object(**self._bucket_key_for(locator))

    async def _list_resources_reorganize_exn(self) -> AsyncIterator[S3ResourceLocator]:
        async for page in self._s3_client.get_paginator('list_objects_v2').paginate(Bucket=self._params.bucketName):
            for obj in page.get("Contents", []):
                yield S3ResourceLocator(
                    key=obj['Key'],
                )
        async for page in self._s3_client.get_paginator('list_multipart_uploads').paginate(Bucket=self._params.bucketName):
            for upload in page.get("Uploads", []):
                # perform clean only in filer server, not here it's not the responsibility of current component
                # await self._s3_client.abort_multipart_upload(Bucket=self._params.bucketName, Key=upload['Key'], UploadId=upload['UploadId'])
                yield S3ResourceLocator(
                    key=upload['Key'],
                    uploadId=upload['UploadId']
                )

    def serialize_backend_failure_exception(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulFilerS3Backend exception',
                retryable=False
            )

        if isinstance(exn, ClientError):
            return BackendFailure(
                failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
                humanMessage=f"FilerException::EffectfulFilerS3Backend boto3 exception: {exn}",
                retryable=False
            )

        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulFilerS3Backend::InternalError',
            retryable=False,
            originalException=exn
        )


if __name__ == "__main__":
    from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, hash_protocol_for_type

    import anyio

    ENDPOINT_URL = "http://localhost:9000"
    ACCESS_KEY = "myappkey"
    SECRET_KEY = "myappsecret123"
    BUCKET = "my-bucket"

    params = FilerBackendS3Parameters(url=ENDPOINT_URL, accessKey=ACCESS_KEY, secretKey=SECRET_KEY, bucketName=BUCKET)

    data = open('C:\\windows\\system32\\wmp.dll', 'rb').read()
    data = data*2
    chosenHashAlg = MixedMd5Sha256()
    with hash_protocol_for_type(chosenHashAlg).compute_new() as h:
        h.update(data)
        hash = h.to_hashed()
        hashlocator = S3ResourceLocator(key=hash.str_serialize())

    async def main():
        efsb = EffectfulFilerS3Backend(params=params)
        async with efsb:
            await efsb.prepare_placeholder_for_hash(hash, 0, 0x10001)
            print(await efsb.upload_terminate_for_hash(hash, 0))

            placeholder_idx = 0
            await efsb.prepare_placeholder_for_hash_exn(hash, placeholder_idx, len(data))
            print(await efsb.upload_chunk_for_hash(hash, placeholder_idx, 0, data[:0x800001]))
            await efsb.upload_chunk_for_hash_exn(hash, placeholder_idx, 0, data[: 0x800000])
            print(await efsb.upload_chunk_for_hash(hash, placeholder_idx, 0x800001, data[0x800000: 0x800000*2]))
            # it's ok to upload the same part multiple times
            await efsb.upload_chunk_for_hash_exn(hash, placeholder_idx, 0, data[: 0x800000])
            print(await efsb.upload_terminate_for_hash(hash, placeholder_idx))  # but all parts have not been uploaded

            await efsb.prepare_placeholder_for_hash_exn(hash, placeholder_idx, len(data))
            print(await efsb.upload_chunk_for_hash(hash, placeholder_idx, 0x800000*5, data[0x800000:]))
            for i in range(0, len(data), 0x800000):
                print(await efsb.upload_chunk_for_hash(hash, placeholder_idx, i, data[i: i+0x800000]))
            await efsb.upload_terminate_for_hash_exn(hash, placeholder_idx)

            async for r in efsb.list_resources_reorganize_exn():
                print(r)

            print(await efsb.size_for_hash(hash))

            downloaded = await efsb.download_chunk_for_hash_exn(hash, 0, 0x100000)
            print(downloaded[:0x40], '[...]', len(downloaded))
            await anyio.sleep(3)
            await efsb.delete_content(hash)

            downloaded = await efsb.download_chunk_for_hash(hash, 0, 0x10000)
            print(downloaded)

    anyio.run(main)
