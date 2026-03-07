from enum import Enum


class Origin(Enum):
    UNIVERSAL = 1
    RFC = 2
    REPLICANT = 3                  # all execution systems who share current communication protocol
    ARBITRARY_CONVENTION = 4       # fixed internal convention to represent common data
    EXTERNAL_EXECUTION_SYSTEM = 5  # some opaque (or not) exe / library understanding the type and providing manipulation api
    EXTERNAL_CONVENTION = 6        # RFC are some instances of this, we may refactor later
