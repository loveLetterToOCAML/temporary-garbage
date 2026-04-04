from identity.identity_config import SecurityContextConfig
from action.action_api import ExternalIPService

from pydantic import BaseModel, SecretStr

from typing import Literal
from enum import Enum

from vault.vault_api import SecretRef


class FilerBackendType(Enum):
    FS = 1
    SQLDatabase = 2
    InMemory = 3
    CloudProvider = 4            # S3, Blob, ...
    ExternalStorageProvider = 5  # gdoc, sharepoint, mail provider, ...


class DefineFromRoot(BaseModel):
    rootSuffix: str = '.filer'

class DefineFromContext(BaseModel):
    rootSuffix: str = '.filer'

class DefineFromContextOrRoot(BaseModel):  # if no context is provided,
    rootSuffix: str = '.filer'

class DefineFromAbs(BaseModel):
    rootPath: str

class FSFiler(BaseModel):
    kind: Literal[FilerBackendType.FS] = FilerBackendType.FS
    lockFileNameSuffix: str = '.lock'
    registryFileName: str = 'registry.json'
    rootLocation: DefineFromRoot | DefineFromContext | DefineFromContextOrRoot | DefineFromAbs = DefineFromRoot()


class SQLiteTableLocator(BaseModel):
    dbFileName: str = '.filer/filer.db'
    dbPassword: str | SecretStr | SecretRef = ''
    parentLocation: DefineFromRoot | DefineFromContext | DefineFromAbs = DefineFromRoot()
    tableNamePrefix: str = 'filer'

class PostgreSQLDatabaseLocator(BaseModel):
    externalServiceLocator: ExternalIPService
    dbSecurityMaterial: SecurityContextConfig
    schemaName: str
    tableNamePrefix: str = 'filer'


class SQLFiler(BaseModel):
    kind: Literal[FilerBackendType.SQLDatabase] = FilerBackendType.SQLDatabase
    TableLocator: SQLiteTableLocator | PostgreSQLDatabaseLocator


class InMemoryFiler(BaseModel):
    kind: Literal[FilerBackendType.InMemory] = FilerBackendType.InMemory
    cacheDirectorySuffix: str = '.filercache'
    maxDataSizeInMemory: int = 0x1000000
