from filer.base_exceptions import FilerSerialException, AlreadyUploadingContent, AlreadyUploadedContent, \
    NotExistingContent
from baseimplems.persistence.sqlalchemy_database import with_auto_session_kwargs, with_auto_session_kwargs_gen
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from basetypes.implementation.dataformat.hashed import Hashed, HashAlgorithm, HashAlgorithmInstance
from baseimplems.persistence.model_utils.model_utils_common import WithBytesHashPrimaryKey, WithID
from baseimplems.persistence.mixins import BaseMixins, commit_and_rollback_if_exception
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from filer.filer_backend.backend_impl_inmem import check_final_content_hash_exn
from filer.filer_backend.interval_union import IntervalUnion
from filer.filer_backend.utils_exn import SerialException

from sqlalchemy import LargeBinary, Enum, select, ForeignKey, delete, func, Integer
from sqlalchemy.testing.schema import mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.exc import NoResultFound
from sortedcontainers import SortedDict
from anyio import Lock

from typing import AsyncIterator


class ContentForHash(*BaseMixins, WithBytesHashPrimaryKey):

    __tablename__ = 'FileContent'

    hash_type: Mapped[HashAlgorithm] = mapped_column(Enum(HashAlgorithm), nullable=False)
    content: Mapped[bytes] = mapped_column( LargeBinary(2**32-1), nullable=False, unique=True)


class TemporaryContentForHash(*BaseMixins, WithID):

    __tablename__ = 'TemporaryFileContent'

    hash_type: Mapped[HashAlgorithm] = mapped_column(Enum(HashAlgorithm), nullable=False)
    hash: Mapped[bytes] = mapped_column('hash', LargeBinary(0x200), nullable=False)  # not primary key to handle multiple same upload intents


class TemporaryChunkForHash(*BaseMixins, WithID):

    __tablename__ = 'TemporaryChunk'

    temporary_content_id: Mapped[int] = mapped_column(ForeignKey(TemporaryContentForHash.id), nullable=False)
    temporary_content = relationship(TemporaryContentForHash, foreign_keys=temporary_content_id, backref=f"__related_chunks")
    content: Mapped[bytes] = mapped_column(LargeBinary(2**32-1), nullable=False)
    start: Mapped[int] = mapped_column(Integer)
    end: Mapped[int] = mapped_column(Integer)


class EffectfulFilerSqlBackend(EffectfulFilerBackend[Hashed, ContentForHash, BackendFailure], EffectfulBackend[ContentForHash, BackendFailure]):

    @property
    def _effectful_backend(self) -> EffectfulBackend[ContentForHash, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: ContentForHash) -> Hashed | None:
        return Hashed(
            hashAlgorithm=HashAlgorithmInstance(type=locator.hash_type),  # todo: handle hashes with parameters
            hash=locator.hash
        )

    def resource_locator_from_hash(self, hash: Hashed) -> ContentForHash:
        return ContentForHash(
            hash_type=hash.hashAlgorithm.type,
            hash=hash.hash
        )


    def __init__(self):
        self._intervals_for_id = {}
        self._expected_total_size_for = {}
        self._lock_per_id = {}
        self._data_slices_per_id = {}
        self._uploaded_size_for = {}

    def _filter_by_hash(self, query, locator: ContentForHash):
        return query.where(
            ContentForHash.hash == locator.hash
        ).where(
            ContentForHash.hash_type == locator.hash_type
        )

    def _filter_temporary_by_hash(self, query, placeholder_index: int):
        return query.where(
            TemporaryContentForHash.id == placeholder_index
        )

    @with_auto_session_kwargs
    async def size_of_content_at_exn(self, locator: ContentForHash, *, session: AsyncSession) -> int:
        stmt = select(
            func.strlen(ContentForHash.content)
        )
        stmt = self._filter_by_hash(stmt, locator)
        return (await session.execute(stmt)).scalar_one()

    @with_auto_session_kwargs
    async def prepare_placeholder_at_exn(self, locator: ContentForHash, placeholder_index: int, total_size: int, *, session: AsyncSession):
        if (await session.execute(self._filter_by_hash(select(ContentForHash), locator))).one_or_none():
            raise FilerSerialException(
                AlreadyUploadedContent(existingUlid=None, hashAttempted=locator.hash)
            )
        # todo: remove below and replace with intelligent get of new index for placeholder (avoid DOS)
        if (await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), placeholder_index))).one_or_none():
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=locator.hash, placeholderIndex=placeholder_index)
            )
        new_obj = TemporaryContentForHash(hash_type=locator.hash_type, hash=locator.hash, id=placeholder_index)
        session.add(new_obj)
        await commit_and_rollback_if_exception(session)
        self._intervals_for_id[new_obj.id] = IntervalUnion()
        self._expected_total_size_for[new_obj.id] = total_size
        self._lock_per_id[new_obj.id] = Lock()
        self._data_slices_per_id[new_obj.id] = SortedDict()
        self._uploaded_size_for[new_obj.id] = 0
        return new_obj

    async def _get_all_slices_for(self, session, interval_id):
        query = select(
            TemporaryChunkForHash.start, TemporaryChunkForHash.end, TemporaryChunkForHash.id
        ).where(
            TemporaryChunkForHash.temporary_content_id == interval_id
        )
        return {(t[0], t[1]): t[2] for t in await session.execute(query)}

    @with_auto_session_kwargs
    async def upload_chunk_at_exn(self, locator: ContentForHash, placeholder_index: int, offset: int, data: bytes, *, session: AsyncSession) -> int:
        current_temporary_file = (
            await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), placeholder_index))
        ).scalar_one()
        interval_id = current_temporary_file.id

        async with self._lock_per_id[interval_id]:
            interval = self._intervals_for_id[interval_id]
            #data_slices = await self._get_all_slices_for(session, interval_id)
            data_slices = self._data_slices_per_id[interval_id]

            interval_tuple = (offset, min(offset + len(data), self._expected_total_size_for[interval_id]))
            intersection: IntervalUnion = interval.intersect(*interval_tuple)
            bytes_updated = 0
            for start, end in intersection.intervals:
                if (start, end) in data_slices:
                    await session.execute(delete(TemporaryChunkForHash).where(TemporaryChunkForHash.id == data_slices[(start, end)]))
                    del data_slices[(start, end)]
                    interval.delete(start, end)
                    bytes_updated += start - end

            intersection_diff: IntervalUnion = interval.intersect_difference(*interval_tuple)
            new_chunks = []
            for start, end in intersection_diff.intervals:
                new_chunks.append(
                    TemporaryChunkForHash(
                        start=start,
                        end=end,
                        temporary_content_id=interval_id,
                        content=data[start - offset: end - offset]
                    )
                )
                session.add(new_chunks[-1])
            await commit_and_rollback_if_exception(session)  # to only modify interval if DB update succeeded
            for (start, end), chunk in zip(intersection_diff.intervals, new_chunks):  # so do the same thing if we did not rollback & raise
                data_slices[(start, end)] = chunk.id
            interval.add(*interval_tuple)
            self._uploaded_size_for[interval_id] += intersection_diff.actual_filled + bytes_updated
            return intersection_diff.actual_filled + bytes_updated


    async def _clean_upload(self, session: AsyncSession, interval_id: int):
        data_slices = self._data_slices_per_id[interval_id]
        for data_slice in data_slices:
            query = delete(TemporaryChunkForHash).where(TemporaryChunkForHash.id == data_slices[data_slice])
            await session.execute(query)
        query = delete(TemporaryContentForHash).where(TemporaryContentForHash.id == interval_id)
        await session.execute(query)

    async def _check_final_and_move(self, session: AsyncSession, interval_id: int, locator: ContentForHash):
        full_bytes = b''  # we are forced to get full bytes here due to how we insert in DB
        data_slices = self._data_slices_per_id[interval_id]
        cur = 0
        for data_slice in data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            query = select(TemporaryChunkForHash).where(TemporaryChunkForHash.id == data_slices[data_slice])
            data = (await session.execute(query)).scalar_one()
            full_bytes += data.content
            cur += len(data.content)

        def gen_bytes_of():
            for i in range(0, 0x1000000, len(full_bytes)):
                yield full_bytes[i: i+0x1000000]

        check_final_content_hash_exn(self.hash_from_resource_locator(locator), gen_bytes_of)
        content = ContentForHash(
            hash_type=locator.hash_type,
            hash=locator.hash,
            content=full_bytes
        )
        session.add(content)
        await commit_and_rollback_if_exception(session)
        await session.execute(delete(TemporaryContentForHash).where(TemporaryContentForHash.id == interval_id))

    @with_auto_session_kwargs
    async def upload_terminate_at_exn(self, locator: ContentForHash, placeholder_index: int, *, session: AsyncSession):
        current_temporary_file = (
            await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), placeholder_index))
        ).scalar_one()
        interval_id = current_temporary_file.id
        async with self._lock_per_id[interval_id]:
            try:
                if self._expected_total_size_for[interval_id] == self._uploaded_size_for[interval_id]:
                    await self._check_final_and_move(session, interval_id, locator)
            finally:
                await self._clean_upload(session, interval_id)
                del self._intervals_for_id[interval_id]
                del self._expected_total_size_for[interval_id]
                del self._lock_per_id[interval_id]
                del self._data_slices_per_id[interval_id]
                del self._uploaded_size_for[interval_id]

    @with_auto_session_kwargs
    async def download_chunk_from_exn(self, locator: ContentForHash, offset: int, size: int, *, session: AsyncSession) -> bytes:
        stmt = select(
            func.substring(ContentForHash.content, offset + 1, size)
        ).where(
            ContentForHash.hash == locator.hash
        ).where(
            ContentForHash.hash_type == locator.hash_type
        )
        return (await session.execute(stmt)).scalar_one()

    @with_auto_session_kwargs
    async def delete_resource_at_exn(self, locator: ContentForHash, placeholder_index: int = -1, *, session: AsyncSession):
        if placeholder_index < 0:
            await session.execute(delete(ContentForHash).where(ContentForHash.hash == locator.hash))
        else:
            await session.execute(delete(TemporaryContentForHash).where(TemporaryContentForHash.hash == locator.hash))

    @with_auto_session_kwargs_gen
    async def _list_resources_reorganize_exn(self, *, session: AsyncSession) -> AsyncIterator[ContentForHash]:
        for hash, hash_type in (await session.execute(select(ContentForHash.hash, ContentForHash.hash_type))):
            yield ContentForHash(
                hash=hash,
                hash_type=hash_type
            )

    def exception_to_serialized_failure(self, exn: Exception) -> BackendFailure:
        if isinstance(exn, SerialException):
            return BackendFailure(
                failure=exn.serialized,
                humanMessage=exn.serialized.humanMessage or 'FilerException::EffectfulFilerFsBackend exception',
                retryable=False
            )
        if isinstance(exn, NoResultFound):
            ser_exn = NotExistingContent(
                inputHash=b'unknown',
            )
            return BackendFailure(
                failure=ser_exn,
                humanMessage='FilerException::EffectfulFilerFsBackend::NotFound',
                retryable=False
            )
        return BackendFailure(
            failure=ExternalFailure(externalFailureType=ExternalFailureType.InternalError),
            humanMessage='FilerException::EffectfulFilerFsBackend::InternalError',
            retryable=False,
            originalException=exn
        )


if __name__ == '__main__':
    from basetypes.implementation.dataformat.hashed import MixedMd5Sha256, hash_protocol_for_type
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy

    import anyio

    import random
    import string


    data = bytes(map(ord, random.choices(string.ascii_letters, k=0x1000)))
    chosenHashAlg = MixedMd5Sha256()
    with hash_protocol_for_type(chosenHashAlg).compute_new() as h:
        h.update(data)
        hash = h.to_hashed()

    async def main():
        async with (
            run_with_temporarily_persistent_mock_db_engine(echo=False),
            run_within_sqlalchemy() as _,
        ):
            ebim = EffectfulFilerSqlBackend()

            placeholder_idx = 0
            await ebim.prepare_placeholder_for_hash_exn(hash, placeholder_idx, len(data))
            for i in range(0, 0x1000, 0x10):
                await ebim.upload_chunk_for_hash_exn(hash, placeholder_idx, i, data[i:i+0x10])
            print("[+] Check now, upload chunks finished")
            #await anyio.sleep(10)
            await ebim.upload_terminate_for_hash_exn(hash, placeholder_idx)
            print("[+] Check now, chunks should be deleted and real content obtained")
            #await anyio.sleep(10)

            print(await ebim.upload_chunk_for_hash(hash, placeholder_idx, i, data[i:i + 0x10]))
            await ebim.prepare_placeholder_for_hash(hash, placeholder_idx, len(data))

            async for r in ebim.list_resources_reorganize_exn():
                print(r)

            downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
            print(downloaded)

            print("[+] Check now, before content being destroyed (10s)")
            #await anyio.sleep(10)
            await ebim.delete_content(hash)
            print(await ebim.download_chunk_for_hash(hash, 0, 0x10000))

            ph = 1
            await ebim.prepare_placeholder_for_hash_exn(hash, ph, len(data))
            s = 0
            x = 0
            while s != 0x1000 and x < 0x30:
                size = random.randint(1, 3)
                beg = random.randint(0, 0xe)
                s += await ebim.upload_chunk_for_hash_exn(hash, ph, beg*0x100, data[beg*0x100:beg*0x100 + size*0x100])
                print(size, beg, s)
                x += 1

            await ebim.upload_terminate_for_hash_exn(hash, ph)
            async for r in ebim.list_resources_reorganize_exn():
                print(r)

    anyio.run(main)
