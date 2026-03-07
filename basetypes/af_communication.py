from enum import Enum


class CanalEndReason(Enum):
    SUCCESS = 0
    SELECT_FAILED = 1
    READ_FAILED = 2
    PARSING_FAILED = 3
    TIMEOUT = 4
    STOP_ASKED = 5
    EXCEPTION_IN_PARENT = 6


class CanalEndSide(Enum):
    SOURCE = 0
    TARGET = 1
    BOTH = 2
    EXTERNAL = 3
    UNKNOWN = 4

