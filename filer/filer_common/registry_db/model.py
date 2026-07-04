from baseimplems.persistence.model_utils.model_utils_common import WithULID, WithStringHash, WithSizeAttributes, \
    TWithStringHash, TWithBytesHash, WithUniqueName, TWithID, WithBytesHashPrimaryKey, \
    WithStringHashPrimaryKey, WithSoftDelete
from baseimplems.persistence.model_utils.high_order_sqlalchemy_registry import register_sqlalchemy_type, \
    default_sqlalchemy_classname_keying
from baseimplems.persistence.model_utils.model_utils_time import CreatedAt

from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy import ForeignKey, String

from typing import Type


registry_metadata_name = 'RegistryMetadata'

@register_sqlalchemy_type(registry_metadata_name, default_sqlalchemy_classname_keying)
def RegistryMetadataTable_for(MetadataType: TWithID, HashType: TWithStringHash | TWithBytesHash | Type | None = None):
    WithAdditional = tuple()
    if HashType is None or HashType is bytes:
        hash_type_name = 'bytes'
        WithAdditional = (WithBytesHashPrimaryKey, )
    elif HashType is str:
        hash_type_name = 'str'
        WithAdditional = (WithStringHashPrimaryKey,)
    elif 'id' in HashType.__table__.columns:  # we must be TWithStringHash or TWithBytesHash otherwise not implemented so we can raise here also
        hash_type_name = HashType.__tablename__
    else:
        raise TypeError(f"{HashType.__name__} has no 'id' column")

    table_name = f"{registry_metadata_name}<{hash_type_name},{MetadataType.__tablename__}>"
    # the other way is to declare a class with __abstract__ = True, and every attribute being a declared_attr to make
    # sqlalchemy happy. The following is more concise:
    metadata_id = mapped_column(ForeignKey(MetadataType.id), nullable=False)
    attrs = {
        "__tablename__": table_name,
        "metadata_id": metadata_id,
        "metadata_instance": relationship(MetadataType, foreign_keys=metadata_id,
                                          backref=f"__registry_for_{hash_type_name}_metadata"),
    }
    if hasattr(HashType, "__table__"):
        hash_id = mapped_column(ForeignKey(HashType.id), nullable=False, primary_key=True)
        attrs["hash_id"] = hash_id
        attrs["hash"] = relationship(HashType, foreign_keys=hash_id,
                                     backref=f"__hash_for_{hash_type_name}_metadata")
    bases = (WithULID, WithSoftDelete, WithSizeAttributes, CreatedAt, *WithAdditional, *BaseMixins)
    return type(attrs['__tablename__'], bases, attrs)


if __name__ == '__main__':
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy
    from baseimplems.persistence.model_utils.model_utils_common import WithID
    from baseimplems.persistence.mixins import BaseMixins

    import anyio


    class Metadata(WithID, WithUniqueName, *BaseMixins):
        __tablename__ = 'Metadata'

        content: Mapped[str] = mapped_column(String(0x10))


    class SpecificHash(WithID, WithStringHash, *BaseMixins):
        __tablename__ = 'SpecificHash'

        myhashpart1: Mapped[str] = mapped_column(String(0x10))
        myhashpart2: Mapped[str] = mapped_column(String(0x10))


    T1 = RegistryMetadataTable_for(Metadata, str)
    T2 = RegistryMetadataTable_for(Metadata, bytes)


    async def main():
        async with (
            run_with_temporarily_persistent_mock_db_engine(),
            run_within_sqlalchemy() as db,
            db,
            db.session() as sess,
        ):
            T3 = RegistryMetadataTable_for(Metadata, SpecificHash)
            print("Creating T3 Specific hash table")
            await db.force_schema_update()  # needed for new T3 table to be created

            m1 = await Metadata.create(name='testmetadata', content='example')
            print(m1)
            sess.add(
                T1(
                    metadata_instance=m1,
                    hash='thisisuniquehash',
                    size=150
                )
            )
            sess.add(
                T1(
                    metadata_instance=m1,
                    hash='thisisuniquehashagain',
                    size=150
                )
            )
            sess.add(
                T2(
                    metadata_instance=m1,
                    hash=b'\x01\x02\xff\x80\x7f',
                    size=1955
                )
            )
            sess.add(
                T3(
                    metadata_instance=m1,
                    hash=await SpecificHash.create(myhashpart1='abcd', myhashpart2='efgh', hash='spechash'),
                    size=150
                )
            )
            await sess.commit()

    anyio.run(main)
