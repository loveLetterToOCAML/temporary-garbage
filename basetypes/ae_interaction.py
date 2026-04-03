from basetypes.a_root import Root, SerialType, SerializationNode

from enum import Enum


class InteractionType(Enum):
    QueryResponse = 1
    OpaqueExchange = 2
    DistributedData = 3
    Exception = 10


### BEGIN AUTO GENERATION
# Auto-generated from InteractionType for auto-completion purpose
class InteractionNode(SerializationNode):
    QueryResponse = ...
    OpaqueExchange = ...
    DistributedData = ...
    Exception = ...
### END AUTO GENERATION


Interaction: InteractionNode = Root.register_serialization_child(SerialType.Interaction, InteractionType)


if __name__ == '__main__':
    from basetypes.autocomplete_helper import generate_autocompletion_for_enum
    print(generate_autocompletion_for_enum(InteractionType))
