from filer.filer_backend.api import FSFiler, SQLFiler, InMemoryFiler
from filer.filer_config import FilerConfigType, FilerConfigContext

from pydantic import BaseModel


class FilerBackendInteractionConfig(BaseModel):
    maxChunkSize: int = 0x100000
    maxDelayPerChunk: int = 3

class FilerBackendConfig(BaseModel):
    needDeletionConfirm: bool = True
    interactionConfig: FilerBackendInteractionConfig = FilerBackendInteractionConfig()
    storageType: FSFiler | SQLFiler | InMemoryFiler = FSFiler()


FilerBackendConfigContext = FilerConfigContext.register_child(
    FilerConfigType.FilerBackend,
    SubconfigEnum=FilerBackendConfig
)
