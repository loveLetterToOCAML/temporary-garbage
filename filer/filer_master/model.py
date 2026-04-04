from filer.filer_backend.api import SQLiteTableLocator, DefineFromRoot, DefineFromAbs, PostgreSQLDatabaseLocator
from filer.filer_config import FilerConfigContext, FilerConfigType

from pydantic import BaseModel


class FilerMasterInteractionConfig(BaseModel):
    maxRequestsPerSecond: int = 0x10

class SQLiteTableLocatorForFileMaster(SQLiteTableLocator):
    dbFileName: str = '.filer_master/filer_master.db'
    parentLocation: DefineFromRoot | DefineFromAbs = DefineFromRoot()

class FilerMasterConfig(BaseModel):
    maxFilers: int = 0x100
    interactionConfig: FilerMasterInteractionConfig = FilerMasterInteractionConfig()
    tableLocator: SQLiteTableLocatorForFileMaster | PostgreSQLDatabaseLocator = SQLiteTableLocatorForFileMaster()


FilerMasterConfigContext = FilerConfigContext.register_child(
    FilerConfigType.FilerMaster,
    BaseConfigAttributesType=FilerMasterConfig,
    with_default=True
)
