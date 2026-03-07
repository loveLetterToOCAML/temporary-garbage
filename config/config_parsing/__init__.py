from pydantic import BaseModel

from enum import Enum

from policy.log import WithLogPolicy


class ConfigParsingType(Enum):
    DYNACONF = 1
    CUSTOM_WITH_INCLUDE_AND_COLORATION = 2


class ConfigParsingPolicy(BaseModel):
    configParsingEngine: ConfigParsingType = ConfigParsingType.CUSTOM_WITH_INCLUDE_AND_COLORATION
    defaultColoration: str = 'default'
    allowOtherConfigFilesInclusion: bool = False
    parseCommandLine: bool = True
    parseEnvironment: bool = True
    # if not set or false, will not allow to parse anything more from a config file, if true will resolve current user home
    rootPath: str | bool | None = True
    defaultConfigName: str = 'main.toml'
    environmentPrefix: str = 'CONFIG'
    supplementalDotenvFile: str | None = None
    attemptDefaultParsingIfException: bool = False
    attemptInteractionIfException: bool = False
    # this avoids storing current policy that should be fixed by env or args and not in main.toml or other config file
    hideDefaultParsingInConfig: bool = True


class ConfigPoliciesConfig():
    pass

class ConfigPolicy(BaseModel, WithLogPolicy, WithInteractionPolicy, WithAuthorizationPolicy):
    configParsingPolicy: ConfigParsingPolicy
    configAuthorizationPolicy: ConfigAuthorizationConfig = ConfigAuthorizationConfig()
    configInteractionConfig: InteractionConfig | None = None
    configLoggingConfig: LoggingConfig | None = None
    configPoliciesConfig: ConfigPolicyConfig

# configLoggingConfig
# = Logging<Config>

# configInteractionConfig
# = Interaction<Config>

parsed_config = register_parsing(Config, Config.Config, CustomConfigParsing)

defaultConfigParsingOptions = CustomConfigParsing()



def parse_config(parser_config: ConfigParsingConfig | None = None):
    base_config_options = parser_config or defaultConfigParsingOptions
    if base_config_options.parseCommandLine:
        parsed_commandline_intent = commandline_context.get()
        if parsed_commandline_intent.config:
            base_config_options = ConfigParsingConfig(
                **base_config_options.model_dump(),
                **parsed_commandline_intent.config.model_dump()
            )
    initial_config = CustomConfigParsing(**base_config_options)

    """
     0: acknowledge config variable if passed as arguments during specific config intent processing
     1: parse environment config variables
     2: parse the configuration file (provided by arg, or then by env or then default location)
     3: for each sub config with "include" field
     4: when context portion is parsed, and when there is a context, one should parse config again to merge it
    """
    match parsing_type:
        case ConfigParsingType.DYNACONF:
            from .dynaconf_parsing import parse_config
            loaded_configuration = parse_config(initial_config)  # shortcut to dynaconf configuration
        case ConfigParsingType.CUSTOM_WITH_INCLUDE_AND_COLORATION:
            from .custom_parsing import parse_config
            loaded_configuration = parse_config(initial_config)
        case _:
            raise NotImplementedError()



if __name__ == "__main__":
    import tomllib

    toml_str = """
    config-coloration = "red"
    propagate-coloration = true
    
    [red.config]
    parse-environment = 1
    
    [red.config.subconfig]
    we-dont-know-yet = true
    
    [red.communication]
    include_from_file = "communication.toml"
    include_config_coloration = "orange"

    [red.filer]
    backend = 1
    
    [yellow.filer]
    backend = 2
    """

    data = tomllib.loads(toml_str)
    print(data)
