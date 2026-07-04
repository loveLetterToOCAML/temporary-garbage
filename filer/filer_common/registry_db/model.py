from baseimplems.persistence.model_utils.model_utils_common import WithULID, WithStringHash, SizeAttributes
from baseimplems.persistence.model_utils.model_utils_time import CreatedAt

from sqlalchemy import ForeignKey, MetaData, Table
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql.type_api import TypeEngine

meta = MetaData()

def RegistryMetadataTable_for(HashType: Table | TypeEngine, MetadataType: Table):

    class DefaultRegistryMetadata(WithULID, WithStringHash, SizeAttributes, CreatedAt):

        __tablename__ = f"registry<{HashType.__tablename__},{MetadataType.__tablename__}>"

        metadata_id: Mapped[MetadataType] = mapped_column(ForeignKey(MetadataType.id), nullable=False)
        metadata: Mapped[MetadataType] = relationship(MetadataType, foreign_keys=[metadata_id], backref='__registry_for_metadata')

    return DefaultRegistryMetadata
