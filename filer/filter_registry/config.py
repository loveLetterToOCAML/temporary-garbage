from filer.filer_proxy.upload_download_proxy_config import UploadDownloadFilerConfig
from filer.filer_backend.filer_backend_config import FilerBackendConfig

from pydantic_extra_types.ulid import ULID
from pydantic import BaseModel

from typing import List


class UniqueFilerBackendConfig(BaseModel):
    filerBackendName: str = 'DEFAULT_LOCAL'  # coupling with default FilerBackendConfig() (local fs)
    filerBackendULID: ULID | None = None
    filerBackend: FilerBackendConfig = FilerBackendConfig()

class FilerRegistryConfig(BaseModel):
    backends: List[UniqueFilerBackendConfig] = [UniqueFilerBackendConfig()]
    proxies: List[UploadDownloadFilerConfig] = [UploadDownloadFilerConfig()]
