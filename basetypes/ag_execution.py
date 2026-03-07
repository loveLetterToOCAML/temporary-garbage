from enum import Enum


class ExecutionState(Enum):
    RUNNING = 1   # READY, ALIVE
    PAUSED = 2
    EXITED = 3

    # other states (more container oriented)
    CREATED = 10
    RESTARTING = 11
    REMOVING = 12
    DEAD = 13  # can only be stated by upper execution layer, with the launched process being not responding

    # yet other states (more k8s oriented)
    READY = 20  # CREATED
    ALIVE = RUNNING  # READY
    STOP = REMOVING
    TERMINATED = EXITED


class ExecutionFailureType(Enum):
    SUCCESS = 0
    UNKNOWN = 1

    # internal processing
    UNEXPECTED_STATE = 10    # general case of internal exceptions
    NOT_IMPLEMENTED = 11
    BAD_PRECONDITION = 12    # this case is supposed to handle exceptions raised due to unfulfilled preconditions
    DEPENDENCY_FAILURE = 13  # handle there used libraries that throw unknown exception

    # external processing (side effect, interact-out related, may link to retry policy)
    OUT_BROKEN =  20        # unable to communicate with remote out interactor
    OUT_TIMEOUT = 21        # remote out interactor is out
    OUT_ERROR = 22          # remote out interactor returns some error
    OUT_PARSING_ERROR = 23  # remote out interactor response cannot be parsed (either before or after OUT_ERROR)

    # external processing (side effect, interact-in related)
    IN_BROKEN = 30         # unable to communicate with remote in interactor
    IN_TIMEOUT = 31        # remote in interactor is out
    IN_PARSING_ERROR = 32  # unable to parse in interactor intent

    # related to other event from upper execution system
    EXTERNAL_STOP = 40
    EXTERNAL_EXCEPTION = 41
    EXTERNAL_TIMEOUT = 42



### States and canal failures
# parent execution system || close execution system with privileges
# -> can start execution unit, pause it, kill it
# parent get events (in case of process failure / crash), or the child itself updating its parent about its status
#
