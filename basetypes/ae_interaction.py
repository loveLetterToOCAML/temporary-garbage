from enum import Enum


class InteractionType(Enum):
    QueryResponse = 1
    OpaqueExchange = 2
    DistributedData = 3
