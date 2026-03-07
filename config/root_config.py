from config.config_context import ConfigContext

from enum import Enum


class RootConfigType(Enum):
    Config = 1
    SelfExecution = 2
    Communication = 3
    Context = 4

    Authority = 5
    Interaction = 6
    Persistence = 7
    DataStream = 8
    Action = 9
    Logging = 10

    Visualisation = 11
    Policy = 12

    ExecutionSystem = 100


RootConfigContext = ConfigContext(RootConfigType)
