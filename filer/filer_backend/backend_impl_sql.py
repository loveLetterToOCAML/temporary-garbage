from sqlalchemy.exc import NoResultFound

from filer.base_exceptions import FilerSerialException, AlreadyUploadingContent, AlreadyUploadedContent, \
    NotExistingContent
from filer.filer_backend.backend_failure import BackendFailure, ExternalFailure, ExternalFailureType
from baseimplems.persistence.model_utils.model_utils_common import WithBytesHashPrimaryKey, WithID
from baseimplems.persistence.mixins import BaseMixins, commit_and_rollback_if_exception
from filer.filer_backend.backend_impl_inmem import check_final_content_hash_exn
from filer.filer_backend.backend_proto import EffectfulBackend, EffectfulFilerBackend
from basetypes.implementation.dataformat.hashed import Hashed, HashAlgorithm, HashAlgorithmInstance
from baseimplems.persistence.sqlalchemy_database import with_auto_session_kwargs, with_auto_session_kwargs_gen
from filer.filer_backend.interval_union import IntervalUnion
from filer.filer_backend.utils_exn import SerialException

from sqlalchemy import LargeBinary, Enum, select, ForeignKey, delete, func, Integer
from sqlalchemy.testing.schema import mapped_column
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, relationship
from sortedcontainers import SortedDict
from pydantic import BaseModel
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


# this is to handle upload properly and implement intent unicity for upload with index that allows for several potential
# upload of the same data by different user and avoid DOS by blocking some hash
class HashedWithIndex(BaseModel):
    index: int | None = None
    hashed: Hashed


class EffectfulFilerSqlBackend(EffectfulFilerBackend[HashedWithIndex, TemporaryContentForHash, BackendFailure], EffectfulBackend[TemporaryContentForHash, BackendFailure]):

    @property
    def _effectful_backend(self) -> EffectfulBackend[TemporaryContentForHash, BackendFailure]:
        return self

    def hash_from_resource_locator(self, locator: TemporaryContentForHash) -> HashedWithIndex | None:
        return HashedWithIndex(
            hashed=Hashed(
                hashAlgorithm=HashAlgorithmInstance(type=locator.hash_type),  # todo: handle hashes with parameters
                hash=locator.hash
            ),
            index=locator.id
        )

    def resource_locator_from_hash(self, hash: HashedWithIndex) -> TemporaryContentForHash:
        return TemporaryContentForHash(
            hash_type=hash.hashed.hashAlgorithm.type,
            hash=hash.hashed.hash,
            id=hash.index
        )


    def __init__(self):
        self._intervals_for_id = {}
        self._expected_total_size_for = {}
        self._lock_per_id = {}
        self._data_slices_per_id = {}
        self._uploaded_size_for = {}

    def _filter_by_hash(self, query, locator: TemporaryContentForHash):
        return query.where(
            ContentForHash.hash == locator.hash
        ).where(
            ContentForHash.hash_type == locator.hash_type
        )

    def _filter_temporary_by_hash(self, query, locator: TemporaryContentForHash):
        return query.where(
            TemporaryContentForHash.id == locator.id
        )

    @with_auto_session_kwargs
    async def size_of_content_at_exn(self, locator: TemporaryContentForHash, *, session: AsyncSession) -> int:
        stmt = select(
            func.strlen(ContentForHash.content)
        )
        stmt = self._filter_by_hash(stmt, locator)
        return (await session.execute(stmt)).scalar_one()

    @with_auto_session_kwargs
    async def prepare_placeholder_at_exn(self, locator: TemporaryContentForHash, total_size: int, *, session: AsyncSession):
        if (await session.execute(self._filter_by_hash(select(ContentForHash), locator))).one_or_none():
            raise FilerSerialException(
                AlreadyUploadedContent(existingUlid=None, hashAttempted=locator.hash)
            )
        # todo: remove below and replace with intelligent get of new index for placeholder (avoid DOS)
        if (await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), locator))).one_or_none():
            raise FilerSerialException(
                AlreadyUploadingContent(hashUploading=locator.hash)
            )
        new_obj = TemporaryContentForHash(hash_type=locator.hash_type, hash=locator.hash)
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
    async def upload_chunk_at_exn(self, locator: TemporaryContentForHash, offset: int, data: bytes, *, session: AsyncSession) -> int:
        current_temporary_file = (
            await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), locator))
        ).scalar_one()
        interval_id = current_temporary_file.id

        async with self._lock_per_id[interval_id]:
            interval = self._intervals_for_id[interval_id]
            #data_slices = await self._get_all_slices_for(session, interval_id)
            data_slices = self._data_slices_per_id[interval_id]
            print(data_slices)

            interval_tuple = (offset, min(offset + len(data), self._expected_total_size_for[interval_id]))
            intersection: IntervalUnion = interval.intersect(*interval_tuple)
            for start, end in intersection.intervals:
                if (start, end) in data_slices:
                    await session.execute(delete(TemporaryChunkForHash).where(TemporaryChunkForHash.id == data_slices[(start, end)]))
                    del data_slices[(start, end)]
                    interval.delete(start, end)

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
            print(data_slices)
            interval.add(*interval_tuple)
            self._uploaded_size_for[interval_id] += intersection_diff.actual_filled
            print("inter diff", intersection_diff, intersection_diff.actual_filled)
            return intersection_diff.actual_filled


    async def _check_final_and_move(self, session: AsyncSession, interval_id: int, locator: TemporaryContentForHash):
        full_bytes = b''  # we are forced to get full bytes here due to how we insert in DB
        data_slices = self._data_slices_per_id[interval_id]
        cur = 0
        for data_slice in data_slices:
            if data_slice[0] != cur:
                raise Exception('Bad bytes interval union')
            query = select(
                TemporaryChunkForHash
            ).where(
                TemporaryChunkForHash.id == data_slices[data_slice]
            )
            data = (await session.execute(query)).scalar_one()
            full_bytes += data.content
            cur += len(data.content)
            await data.delete(commit=False)

        def gen_bytes_of():
            for i in range(0, 0x1000000, len(full_bytes)):
                yield full_bytes[i: i+0x1000000]

        check_final_content_hash_exn(self.hash_from_resource_locator(locator).hashed, gen_bytes_of)
        content = ContentForHash(
            hash_type=locator.hash_type,
            hash=locator.hash,
            content=full_bytes
        )
        session.add(content)
        await commit_and_rollback_if_exception(session)
        print("CREATED CONTENT", content.hash)
        await session.execute(delete(TemporaryContentForHash).where(TemporaryContentForHash.id == locator.id))

    @with_auto_session_kwargs
    async def upload_terminate_at_exn(self, locator: TemporaryContentForHash, *, session: AsyncSession):
        current_temporary_file = (
            await session.execute(self._filter_temporary_by_hash(select(TemporaryContentForHash), locator))
        ).scalar_one()
        interval_id = current_temporary_file.id
        async with self._lock_per_id[interval_id]:
            try:
                if self._expected_total_size_for[interval_id] == self._uploaded_size_for[interval_id]:
                    await self._check_final_and_move(session, interval_id, locator)
            finally:
                del self._intervals_for_id[interval_id]
                del self._expected_total_size_for[interval_id]
                del self._lock_per_id[interval_id]
                del self._data_slices_per_id[interval_id]
                del self._uploaded_size_for[interval_id]

    @with_auto_session_kwargs
    async def download_chunk_from_exn(self, locator: TemporaryContentForHash, offset: int, size: int, *, session: AsyncSession) -> bytes:
        stmt = select(
            func.substring(ContentForHash.content, offset + 1, size)
        ).where(
            ContentForHash.hash == locator.hash
        ).where(
            ContentForHash.hash_type == locator.hash_type
        )
        return (await session.execute(stmt)).scalar_one()

    @with_auto_session_kwargs
    async def delete_resource_at_exn(self, locator: TemporaryContentForHash, placeholder: bool = False, *, session: AsyncSession):
        if not placeholder:
            await session.execute(delete(ContentForHash).where(ContentForHash.hash == locator.hash))
        else:
            await session.execute(delete(TemporaryContentForHash).where(TemporaryContentForHash.hash == locator.hash))

    @with_auto_session_kwargs_gen
    async def _list_resources_exn(self, *, session: AsyncSession) -> AsyncIterator[TemporaryContentForHash]:
        for hash, hash_type in (await session.execute(select(ContentForHash.hash, ContentForHash.hash_type))):
            yield TemporaryContentForHash(
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
        print(type(exn), exn)
        raise exn
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
        hash = HashedWithIndex(
            hashed=hash
        )

    async def main():
        async with (
            run_with_temporarily_persistent_mock_db_engine(echo=False),
            run_within_sqlalchemy() as db,
        ):
            ebim = EffectfulFilerSqlBackend()

            ph = await ebim.prepare_placeholder_for_hash_exn(hash, len(data))
            print(ph)
            hash.index = ph.id
            for i in range(0, 0x1000, 0x10):
                await ebim.upload_chunk_for_hash_exn(hash, i, data[i:i+0x10])
            print("[+] Check now, upload chunks finished")
            await anyio.sleep(10)
            await ebim.upload_terminate_for_hash_exn(hash)
            print("[+] Check now, chunks should be deleted and real content obtained")
            await anyio.sleep(10)

            print(await ebim.upload_chunk_for_hash(hash, i, data[i:i + 0x10]))
            await ebim.prepare_placeholder_for_hash(hash, len(data))

            async for r in ebim.list_resources_exn():
                print(r)

            downloaded = await ebim.download_chunk_for_hash(hash, 0, 0x10000)
            print(len(downloaded))

            print("[+] Check now, before content being destroyed (10s)")
            await anyio.sleep(10)
            print(await ebim.delete_content(hash))
            print(await ebim.download_chunk_for_hash(hash, 0, 0x10000))

            ph = await ebim.prepare_placeholder_for_hash_exn(hash, len(data))
            print(ph)
            hash.index = ph.id
            s = 0
            x = 0
            while s != 0x1000 and x < 0x20:
                size = random.randint(1, 3)
                beg = random.randint(0, 0xe)
                s += await ebim.upload_chunk_for_hash_exn(hash, beg*0x100, data[beg*0x100:beg*0x100 + size*0x100])
                print(size, beg, s)
                x += 1

    anyio.run(main)
