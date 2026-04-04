from communication.communication_config import KnownServiceLocatorConfig
from filer.filer_config import FilerConfigContext, FilerConfigType
from cache.cache_config import CacheStrategyConfig

from pydantic import BaseModel


class FilerClientInteractionConfig(BaseModel):
    # will refuse to upload chunks if requested size is not between below min and max
    minChunkSize: int = 0x1000
    maxChunkSize: int = 0x100000

    maxDelayPerChunk: int = 3  # abort chunk upload or download if it takes more than this delay

class FilerClientCacheConfig(BaseModel):
    localCacheEnabled: bool = True
    cacheStrategyConfig: CacheStrategyConfig = CacheStrategyConfig()

class FilerClientConfig(BaseModel):
    filerUploadDownloadProxy: KnownServiceLocatorConfig
    preferredFilerRegistryForUpload: KnownServiceLocatorConfig

    interactionConfig: FilerClientInteractionConfig = FilerClientInteractionConfig()
    cacheConfig: FilerClientCacheConfig = FilerClientCacheConfig()


FilerBackendConfigContext = FilerConfigContext.register_child(
    FilerConfigType.FilerClient,
    FilerClientConfig,
    with_default=True
)
