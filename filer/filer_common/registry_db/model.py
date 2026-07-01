from sqlalchemy.ext.asyncio import AsyncAttrs

from filer.filer_common.registry_db.model_utils_common import WithULID, WithStringHash, SizeAttributes
from filer.filer_common.registry_db.model_utils_time import CreatedAt
from filer.filer_common.registry_db.utils import register_type

from sqlalchemy import Column, Integer, ForeignKey, DateTime, Boolean, MetaData, Table, String
from sqlalchemy.orm import relationship, declared_attr, DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql.type_api import TypeEngine
from ulid import ULID


meta = MetaData()

def RegistryMetadataTable_for(HashType: Table | TypeEngine, MetadataType: Table):

    class DefaultRegistryMetadata(WithULID, WithStringHash, SizeAttributes, CreatedAt):

        __tablename__ = f"registry<{HashType.__tablename__},{MetadataType.__tablename__}>"

        metadata_id: Mapped[MetadataType] = mapped_column(ForeignKey(MetadataType.id), nullable=False)
        metadata: Mapped[MetadataType] = relationship(MetadataType, foreign_keys=[metadata_id], backref='__registry_for_metadata')

    return DefaultRegistryMetadata
