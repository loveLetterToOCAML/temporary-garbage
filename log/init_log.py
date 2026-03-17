from context.log.logging_context import current_log_policy
from policy.log import LogPolicy


# put this not in logger to avoid coupling the effect logging with the context architecture
InitialLoggingContext = register_constant_init(
    'initial_logging_context', LogPolicy(), current_log_policy,
    produce=LogPolicy
)
