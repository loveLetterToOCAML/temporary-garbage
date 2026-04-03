from enum import Enum, EnumType
from typing import Type


class SerialType(Enum):
    Optimized = 1       # specific convention between communication systems to optimize size of produced serialization
    BaseTypes = 2       # all common types including per-domain shared conventions (str with constraints as instance)
    GenericsData = 3    # ability to serialize generics with chosen serialization types

    Authority = 4       # includes authentication, identification / session, access controls, in a global domain
    Interaction = 5     # includes interaction related, result, error, exception, ...
    Communication = 6   # includes network related (routing), secure related, interaction (results, errors)
    SelfExecution = 7   # includes sup and inf execution context, self perception of current execution

    DataFlow = 10
    Action = 11
    Persist = 12
    Config = 13         # consists of intent, policies and context controls for initial config parsing from external to internal API
    Context = 14
    Infra = 15
    Logging = 16
    Policy = 17         # consists of making available read-only objects that represent current centralized policies at any point in time
    Visualization = 18

    Craft = 20
    Test = 21
    Replicate = 22
    Enumerate = 23

    ModelExternal = 30
    Instrument = 31
    Measure = 32

    Understand = 40
    Explore = 41

    ExecutionSystem = 100

    Other = 0xff


class SerializationNode:

    def __init__(self, ChildEnum: EnumType, current_path: bytes | None = None):
        self._ChildEnumType = ChildEnum
        self._path = current_path or b''

    def register_serialization_child(self, enum_value: Enum, ChildEnum: EnumType,
                                     OptionalSerializationNodeChild: Type | None = None):
        assert isinstance(enum_value, self._ChildEnumType)
        assert getattr(self, enum_value.name, None) in (..., None), \
            f"Enum value {enum_value} already in children for current serialization node at {self._path.hex()}"

        child = (OptionalSerializationNodeChild or SerializationNode) \
                    (ChildEnum, self.path_until() + bytes([enum_value.value]))
        setattr(self, enum_value.name, child)
        return child

    def path_until(self, member_enum: Enum | None = None) -> bytes:
        return self._path if not member_enum else self._path + bytes([member_enum.value])


### BEGIN AUTO GENERATION
# Auto-generated from SerialType for auto-completion purpose
class Serial(SerializationNode):
    Optimized = ...
    BaseTypes = ...
    GenericsData = ...
    Authority = ...
    Interaction = ...
    Communication = ...
    SelfExecution = ...
    DataFlow = ...
    Action = ...
    Persist = ...
    Config = ...
    Context = ...
    Infra = ...
    Logging = ...
    Policy = ...
    Visualization = ...
    Craft = ...
    Test = ...
    Replicate = ...
    Enumerate = ...
    ModelExternal = ...
    Instrument = ...
    Measure = ...
    Understand = ...
    Explore = ...
    ExecutionSystem = ...
    Other = ...
### END AUTO GENERATION


Root = Serial(SerialType)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(SerialType))
