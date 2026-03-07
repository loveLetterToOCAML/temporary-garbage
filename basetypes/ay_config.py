from typing import List

from a_root import SerialType, Root
from ab_basetypes import BaseTypes

from pydantic import BaseModel

from enum import Enum


class ConfigTypes(Enum):
    # Optimized = 1       # currently none
    # BaseTypes = 2
    GenericsData = 3    # Generics related to config

    Authority = 4       # authority model for config, dedicated rbac
    Interaction = 5     # config related interactions and intents
    # Communication = 6
    # SelfExecution = 7

    Data = 10           # mainly knowledge ? or filer ? for what ?
    Action = 11         # includes tests on external environment declared at config level
    Persist = 12        # includes persistence management for config
    Config = 13         # config of config parsing, authority, logging, interaction and context
    Context = 14        # config objects that can be manipulated within context
    # Infra = 15
    Visualization = 16  # why not

    Craft = 20          # why not later
    Test = 21           # ability to test a config without touching current environment
    # Replicate = 22
    # Understand = 23
    # Exploration = 24

    ModelExternal = 30  # why not later
    # Instrument = 31
    # Measure = 32


Config = Root.register_serialization_child(SerialType.Config, ConfigTypes)


class GenericsConfigType(Enum):
    ConfigTemplate = 1
    ConfigInstance = 2
    ConfigType = 3
    ConfigDefault = 4
    ConfigMandatory = 5


class ConfigTemplateQuery(BaseModel):
    ofType: BaseTypes.TYPE

class ConfigTemplateAnswer(BaseModel):
    configTree: GenericsType.TREE



# tree for displaying current config

class LazyConfig(BaseModel):
    includesRef: str
    coloration: str | None = None

class ConfigLeaf(BaseModel):
    attributeName: str
    attributeValue: None | bool | int | float | str | datetime | LazyConfig

class ConfigNode:
    nodeName: str
    children: List[ConfigNode | ConfigLeaf]

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


