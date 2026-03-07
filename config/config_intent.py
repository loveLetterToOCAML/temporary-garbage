from pydantic import BaseModel

from basetypes.ab_basetypes import BaseTypes


class FromNodeModel(BaseModel):
    fromNode: TreePath | None = None


# Get config of current execution system
class GetCurrentConfig(FromNodeModel):
    forceFullResolve: bool = False

class GetConfigTemplateExample(FromNodeModel):
    withAllCasesForEnum: bool = True        # this prints all case values for enums with A | B | C syntax
    forceOptionalToInstances: bool = False  # this allows to print and develop every sub object which can be None otherwise

class GetConfigTemplateTyping(FromNodeModel):
    withDefaultValues: bool = True         # this prints default value for each attribute when existing
    withMandatoryValues: bool = True       # this indicates if the value must be present for the config being valid

class AttemptParseConfig(FromNodeModel):
    inputConfig: DictInputData  # includes ref AND interpretation system
    resolveOtherConfigRefs: bool = False
    collectAllErrors: bool = False
    expectConfigOutput: bool = False

class UpdateCurrentConfig(FromNodeModel):
    inputConfig: DictInputData

class IsolateConfigFrom(FromNodeModel):
    destinationReference: ReferenceURL


# tree for displaying current config

class LazyConfig(BaseModel):
    includesRef: str
    coloration: str | None = None

class ConfigLeaf(BaseModel):
    attributeName: str
    attributeValue: None | bool | int | float | str | datetime | LazyConfig

class ConfigNode:
    nodeName: str
    children: GenericTypes [ConfigNode | ConfigLeaf]

ConfigOutput = Tree[ConfigNode, ConfigLeaf]  # for serialization simplification and space economy


# tree for displaying template example

class ConfigTemplateExampleLeaf(BaseModel):
    attributeName: str
    attributeValue: None | bool | int | float | str | datetime | EnumValues

class ConfigTemplateExampleNode:
    nodeName: str
    children: List[ConfigTemplateExampleNode | ConfigTemplateExampleLeaf]


# tree for displaying template typing

class ConfigTemplateTypingLeaf(BaseModel):
    attributeName: str
    attributeValue: None | bool | int | float | str | datetime | EnumValues

class ConfigTemplateTypingNode:
    nodeName: str
    children: List[ConfigTemplateExampleNode | ConfigTemplateExampleLeaf]





ConfigIntentParsingHelp = {
    'fromNode': ParsingOption(Help='')
}

ConfigIntent = GetCurrentConfig | GetConfigTemplateExample | GetConfigTemplateTyping | AttemptParseConfig | UpdateCurrentConfig

def deal_with_config_intent(ci: ConfigIntent):
    match ci:
        case GetCurrentConfig():
            return current_config.get()
        case GetConfigTemplateExample():
            return current_config.get()
        case GetConfigTemplateTyping():
            return current_config.get()
        case AttemptParseConfig():
            return current_config.get()
        case UpdateCurrentConfig():
            return current_config.get()
        case _:
            raise NotImplementedError


if __name__ == '__main__':
    # auto-construction of input parsing intent from base types
    print(python_code_for_intent(
        'ConfigIntentType',
        GetCurrentConfig, GetConfigTemplateExample, GetConfigTemplateTyping, AttemptParseConfig, UpdateCurrentConfig
    ))
    # auto-construction of the serialization protocol related to config intent
    print(python_code_for_communication(
        'ConfigIntentType',
        GetCurrentConfig, GetConfigTemplateExample, GetConfigTemplateTyping, AttemptParseConfig, UpdateCurrentConfig,
        attributes_from_int=20,
    ))
